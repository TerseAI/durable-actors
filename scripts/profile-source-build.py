"""Profile the same actor source ZIP on four Cloud Build machine types using gcloud credentials."""

import argparse
import base64
import hashlib
import json
import subprocess
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

MACHINES = ["E2_MEDIUM", "E2_STANDARD_2", "E2_HIGHCPU_8", "E2_HIGHCPU_32"]


def request(url, token=None, data=None, method=None):
    headers = {"Authorization": "Bearer " + token} if token else {}
    if isinstance(data, dict):
        data = json.dumps(data).encode()
        headers["Content-Type"] = "application/json"
    try:
        with urlopen(
            Request(url, data=data, headers=headers, method=method), timeout=90
        ) as response:
            return response.read()
    except HTTPError as error:
        # Request bodies contain credentials and must never be written to the report.
        raise RuntimeError(f"Google API request failed: HTTP {error.code}") from None


def oauth():
    return subprocess.check_output(
        ["gcloud", "auth", "print-access-token"], text=True
    ).strip()


def put(token, bucket, name, data):
    query = urlencode({"uploadType": "media", "name": name, "ifGenerationMatch": "0"})
    return json.loads(
        request(
            f"https://storage.googleapis.com/upload/storage/v1/b/{bucket}/o?{query}",
            token,
            data,
        )
    )


def get(token, bucket, name, generation=None):
    query = {"alt": "media"}
    if generation:
        query["generation"] = str(generation)
    return request(
        f"https://storage.googleapis.com/storage/v1/b/{bucket}/o/{quote(name, safe='')}?{urlencode(query)}",
        token,
    )


def scoped(token, bucket, source, artifacts, dependencies):
    def rule(roles, expression):
        return {
            "availableResource": f"//storage.googleapis.com/projects/_/buckets/{bucket}",
            "availablePermissions": [f"inRole:roles/storage.{role}" for role in roles],
            "availabilityCondition": {"expression": expression},
        }

    def obj(name):
        return json.dumps(f"projects/_/buckets/{bucket}/objects/{name}")

    boundary = {
        "accessBoundary": {
            "accessBoundaryRules": [
                rule(["objectViewer"], f"resource.name == {obj(source)}"),
                rule(["objectCreator"], f"resource.name.startsWith({obj(artifacts)})"),
                rule(
                    ["objectViewer", "objectCreator"],
                    f"resource.name.startsWith({obj(dependencies)})",
                ),
            ]
        }
    }
    data = urlencode(
        {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "subject_token": token,
            "options": json.dumps(boundary),
        }
    ).encode()
    return json.loads(request("https://sts.googleapis.com/v1/token", data=data))[
        "access_token"
    ]


def milliseconds(start, end):
    return round(
        (
            datetime.fromisoformat(end.replace("Z", "+00:00"))
            - datetime.fromisoformat(start.replace("Z", "+00:00"))
        ).total_seconds()
        * 1000
    )


def profile(args, machine, trial, mode, prefix):
    token = oauth()
    contents = Path(args.source).read_bytes()
    run = f"{prefix}/{machine}/{trial}/{mode}/"
    started = time.monotonic()
    source = put(token, args.bucket, run + "source.zip", contents)
    upload_ms = round((time.monotonic() - started) * 1000)
    artifacts, dependencies = (
        run + "artifacts/",
        f"{prefix}/{machine}/{trial}/dependencies/",
    )
    build_request = {
        "source": {
            "sha256": hashlib.sha256(contents).hexdigest(),
            "entrypoint": args.entrypoint,
            "object": {
                "bucket": args.bucket,
                "name": source["name"],
                "generation": source["generation"],
            },
        },
        "bucket": args.bucket,
        "artifactPrefix": artifacts,
        "dependencyPrefix": dependencies,
        "accessToken": scoped(
            token, args.bucket, source["name"], artifacts, dependencies
        ),
    }
    specification = {
        "steps": [
            {
                "name": args.image,
                "entrypoint": "python3",
                "args": ["/opt/durable-actors/source-build.py"],
                "env": [
                    "DURABLE_ACTORS_BUILD_REQUEST="
                    + json.dumps(build_request).replace("$", "$$")
                ],
            }
        ],
        "serviceAccount": f"projects/{args.project}/serviceAccounts/{args.service_account}",
        "options": {"machineType": machine, "logging": "CLOUD_LOGGING_ONLY"},
        "timeout": "900s",
        "queueTtl": "300s",
        "tags": ["actor-source-profile"],
    }
    parent = f"https://cloudbuild.googleapis.com/v1/projects/{args.project}/locations/{args.region}/builds"
    created = json.loads(request(parent, token, specification))
    identity = created["metadata"]["build"]["id"]
    print(f"{machine} trial={trial} {mode}: {identity}", flush=True)
    deadline = time.monotonic() + 1230
    while True:
        build = json.loads(request(parent + "/" + identity, token))
        if build["status"] not in ("QUEUED", "PENDING", "WORKING"):
            break
        if time.monotonic() > deadline:
            request(parent + "/" + identity + ":cancel", token, {})
            raise RuntimeError("Profile exceeded deadline")
        time.sleep(2)
    result = {
        "machine": machine,
        "trial": trial,
        "mode": mode,
        "id": identity,
        "status": build["status"],
        "uploadMs": upload_ms,
        "createTime": build["createTime"],
        "startTime": build.get("startTime"),
        "finishTime": build.get("finishTime"),
        "timing": build.get("timing"),
        "steps": [
            {
                key: step[key]
                for key in ("timing", "pullTiming", "status")
                if key in step
            }
            for step in build["steps"]
        ],
    }
    if build["status"] == "SUCCESS":
        output = json.loads(get(token, args.bucket, artifacts + "result.json"))
        result.update(output)
        result["queueMs"] = milliseconds(build["createTime"], build["startTime"])
        result["buildTotalMs"] = milliseconds(build["createTime"], build["finishTime"])
        result["workflowMs"] = round((time.monotonic() - started) * 1000)
        for item in output["manifest"]["files"]:
            data = get(token, args.bucket, item["object"], item["generation"])
            assert (
                base64.urlsafe_b64encode(hashlib.sha256(data).digest())
                .decode()
                .rstrip("=")
                == item["sha256"]
            )
        result["verifiedArtifacts"] = True
        assert output["dependencyCacheHit"] == (mode == "warm"), output
        print(
            f"{machine} trial={trial} {mode}: {result['buildTotalMs'] / 1000:.1f}s, phases={output['timings']}",
            flush=True,
        )
    else:
        try:
            result["error"] = json.loads(
                get(token, args.bucket, artifacts + "result.json")
            )
        except RuntimeError:
            result["error"] = build.get("failureInfo", {})
        print(
            f"{machine} trial={trial} {mode}: {result['status']} {result['error']}",
            flush=True,
        )
    Path(args.output, f"{machine}-{trial}-{mode}.json").write_text(
        json.dumps(result, indent=2) + "\n"
    )
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for argument in (
        "project",
        "region",
        "bucket",
        "image",
        "service-account",
        "source",
        "entrypoint",
        "output",
    ):
        parser.add_argument("--" + argument, required=True)
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--machines", nargs="+", choices=MACHINES, default=MACHINES)
    args = parser.parse_args()
    Path(args.output).mkdir(parents=True, exist_ok=True)
    prefix = "profiling/source-build/" + str(uuid.uuid4())
    Path(args.output, "run.json").write_text(
        json.dumps(
            {
                "prefix": prefix,
                "image": args.image,
                "region": args.region,
                "sourceSha256": hashlib.sha256(
                    Path(args.source).read_bytes()
                ).hexdigest(),
                "machines": args.machines,
                "trials": args.trials,
            },
            indent=2,
        )
    )
    for trial in range(1, args.trials + 1):
        for mode in ("cold", "warm"):
            with ThreadPoolExecutor(max_workers=4) as pool:
                results = list(
                    pool.map(
                        lambda machine, trial=trial, mode=mode: profile(
                            args, machine, trial, mode, prefix
                        ),
                        args.machines,
                    )
                )
            if any(item["status"] != "SUCCESS" for item in results):
                raise RuntimeError(
                    "Profiling stopped after an unsuccessful build; see result files"
                )


if __name__ == "__main__":
    main()
