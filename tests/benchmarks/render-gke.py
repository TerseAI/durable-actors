from pathlib import Path
import argparse
import base64
import json
import secrets
import subprocess

parser = argparse.ArgumentParser(
    description="Render an isolated GKE WebSocket benchmark from an existing control-plane deployment"
)
parser.add_argument("--reference", type=Path, required=True)
parser.add_argument("--image", required=True)
parser.add_argument("--actor-cpu-millis", type=int, default=2000)
parser.add_argument("--name", required=True)
parser.add_argument("--namespace", required=True)
parser.add_argument("--directory", type=Path, required=True)
args = parser.parse_args()
name, ns, directory, image = args.name, args.namespace, args.directory, args.image
directory.mkdir(parents=True, exist_ok=True)
reference = json.loads(args.reference.read_text())
network_name = reference["spec"]["template"]["metadata"]["labels"][
    "app.kubernetes.io/name"
]
labels = {"benchmark": name}
cp_labels = {
    **labels,
    "app.kubernetes.io/name": network_name,
    "app.kubernetes.io/component": name,
}
items = []


def add(kind, n, spec=None, **fields):
    x = {
        "apiVersion": "apps/v1" if kind in ["Deployment", "StatefulSet"] else "v1",
        "kind": kind,
        "metadata": {"name": n, "namespace": ns, "labels": dict(labels)},
        **fields,
    }
    if spec is not None:
        x["spec"] = spec
    items.append(x)


def service(n, selector, ports, headless=False):
    add(
        "Service",
        n,
        {
            "selector": selector,
            "ports": ports,
            **({"clusterIP": "None"} if headless else {}),
        },
    )


origin = f"https://{name}.{ns}.svc.cluster.local"
(directory / "tls.key").touch(mode=0o600)
subprocess.run(
    [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "2",
        "-keyout",
        str(directory / "tls.key"),
        "-out",
        str(directory / "tls.crt"),
        "-subj",
        f"/CN={name}.{ns}.svc.cluster.local",
        "-addext",
        f"subjectAltName=DNS:{name}.{ns}.svc.cluster.local",
    ],
    check=True,
    stdout=subprocess.DEVNULL,
    stderr=subprocess.DEVNULL,
)
jwt = subprocess.check_output(
    ["openssl", "genpkey", "-algorithm", "ED25519", "-outform", "DER"]
)
credentials = {
    "api-key": secrets.token_urlsafe(32),
    "jwt-signing-key": base64.b64encode(jwt).decode(),
    "postgres-password": secrets.token_urlsafe(24),
}
credentials["postgres-url"] = (
    f"postgres://postgres:{credentials['postgres-password']}@{name}-db:5432/postgres"
)
(directory / "credentials.json").touch(mode=0o600)
(directory / "credentials.json").write_text(json.dumps(credentials))
add("Secret", name, stringData=credentials)
add(
    "Secret",
    name + "-tls",
    data={
        k: base64.b64encode((directory / k).read_bytes()).decode()
        for k in ["tls.crt", "tls.key"]
    },
)
nginx = """worker_processes 2;
worker_rlimit_nofile 200000;
events { worker_connections 65536; }
http { access_log off; error_log /dev/stderr warn; client_max_body_size 64m;
server { listen 8443 ssl; ssl_certificate /tls/tls.crt; ssl_certificate_key /tls/tls.key;
location / { proxy_pass http://127.0.0.1:7100; proxy_http_version 1.1; proxy_set_header Upgrade $http_upgrade; proxy_set_header Connection "upgrade"; proxy_read_timeout 3600s; proxy_send_timeout 3600s; proxy_buffering off; } } }
"""
add(
    "ConfigMap",
    name + "-scripts",
    data={"nginx.conf": nginx, "worker.mjs": (directory / "worker.mjs").read_text()},
)


def secret_env(key, field):
    return {"name": key, "valueFrom": {"secretKeyRef": {"name": name, "key": field}}}


add(
    "Pod",
    name + "-db",
    {
        "containers": [
            {
                "name": "postgres",
                "image": "postgres:16",
                "env": [secret_env("POSTGRES_PASSWORD", "postgres-password")],
                "ports": [{"containerPort": 5432}],
                "resources": {
                    "requests": {"cpu": "100m", "memory": "256Mi"},
                    "limits": {"memory": "1Gi"},
                },
                "readinessProbe": {
                    "exec": {"command": ["pg_isready", "-U", "postgres"]},
                    "periodSeconds": 2,
                },
            }
        ]
    },
)
items[-1]["metadata"]["labels"]["role"] = "db"
service(name + "-db", {"benchmark": name, "role": "db"}, [{"port": 5432}])
prod = reference["spec"]["template"]["spec"]
env = {
    e["name"]: e
    for e in prod["containers"][0]["env"]
    if e["name"] != "DURABLE_ACTORS_SOCKET_EVENT_URL"
}
overrides = {
    "DURABLE_ACTORS_CONTROL_PLANE_URL": f"http://{name}.{ns}.svc.cluster.local:7100",
    "DURABLE_ACTORS_PUBLIC_URL": origin,
    "DURABLE_ACTORS_RUNTIME_IMAGE": image,
    "DURABLE_ACTORS_SPARE_IDLE": "0",
    "DURABLE_ACTORS_SPARE_FLEET_MAX": "8",
    "DURABLE_ACTORS_SPARE_MAX_STARTING": "2",
    "DURABLE_ACTORS_HOST_CPU_MILLIS": str(args.actor_cpu_millis),
    "DURABLE_ACTORS_HOST_MEMORY_MIB": "2048",
    "DURABLE_ACTORS_HOST_IDLE_TIMEOUT_MS": "1000",
    "DURABLE_ACTORS_SOCKET_MAX_CONNECTIONS": "32768",
    "DURABLE_ACTORS_JWT_MAX_TTL_SECONDS": "7200",
}
for k, v in overrides.items():
    env[k] = {"name": k, "value": v}
for k, v in [
    ("DURABLE_ACTORS_POSTGRES_URL", "postgres-url"),
    ("DURABLE_ACTORS_JWT_SIGNING_KEY", "jwt-signing-key"),
    ("DURABLE_ACTORS_SECRET", "api-key"),
]:
    env[k] = secret_env(k, v)
env["DURABLE_ACTORS_GATEWAY_IP"] = {"name": "DURABLE_ACTORS_GATEWAY_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}}
env["DURABLE_ACTORS_GATEWAY_ROUTE"] = {"name": "DURABLE_ACTORS_GATEWAY_ROUTE", "value": "http://$(DURABLE_ACTORS_GATEWAY_IP):7100"}
cp = {
    "name": "control-plane",
    "image": image,
    "env": list(env.values()),
    "resources": {
        "requests": {"cpu": "1", "memory": "1Gi"},
        "limits": {"memory": "4Gi"},
    },
    "ports": [{"containerPort": 7100}],
    "readinessProbe": {
        "httpGet": {"path": "/healthz", "port": 7100},
        "periodSeconds": 3,
    },
    "livenessProbe": {
        "httpGet": {"path": "/healthz", "port": 7100},
        "periodSeconds": 10,
        "failureThreshold": 3,
    },
}
proxy = {
    "name": "tls",
    "image": "nginx:1.27",
    "ports": [{"containerPort": 8443}],
    "volumeMounts": [
        {
            "name": "scripts",
            "mountPath": "/etc/nginx/nginx.conf",
            "subPath": "nginx.conf",
        },
        {"name": "tls", "mountPath": "/tls"},
    ],
    "resources": {
        "requests": {"cpu": "250m", "memory": "128Mi"},
        "limits": {"memory": "2Gi"},
    },
}
volumes = [
    {"name": "scripts", "configMap": {"name": name + "-scripts"}},
    {"name": "tls", "secret": {"secretName": name + "-tls"}},
]
add(
    "Deployment",
    name,
    {
        "replicas": 2,
        "selector": {"matchLabels": {"app.kubernetes.io/component": name}},
        "template": {
            "metadata": {"labels": cp_labels},
            "spec": {
                "serviceAccountName": prod["serviceAccountName"],
                "containers": [cp, proxy],
                "volumes": volumes,
            },
        },
    },
)
service(
    name,
    {"app.kubernetes.io/component": name},
    [
        {"name": "internal", "port": 7100},
        {"name": "tls", "port": 443, "targetPort": 8443},
    ],
)
worker_env = [
    {"name": "DURABLE_ACTORS_CONTROL_PLANE_URL", "value": origin},
    {"name": "DURABLE_ACTORS_PROJECT_ID", "value": name},
    {"name": "BENCH_ACTOR_ID", "value": "single-room"},
    {"name": "BENCH_ACTOR_CPU_MILLIS", "value": str(args.actor_cpu_millis)},
    {"name": "NODE_EXTRA_CA_CERTS", "value": "/tls/tls.crt"},
    secret_env("DURABLE_ACTORS_SECRET", "api-key"),
]
load_name = name + "-load"
add(
    "StatefulSet",
    load_name,
    {
        "replicas": 4,
        "serviceName": load_name,
        "podManagementPolicy": "Parallel",
        "selector": {"matchLabels": {"app": load_name}},
        "template": {
            "metadata": {"labels": {"app": load_name, **labels}},
            "spec": {
                "terminationGracePeriodSeconds": 2,
                "containers": [
                    {
                        "name": "load",
                        "image": "node:22.19.0-bookworm-slim",
                        "command": ["node", "/scripts/worker.mjs"],
                        "readinessProbe": {
                            "httpGet": {"path": "/stats", "port": 8080},
                            "periodSeconds": 1,
                        },
                        "env": worker_env,
                        "ports": [{"containerPort": 8080}],
                        "volumeMounts": [
                            {"name": "scripts", "mountPath": "/scripts"},
                            {"name": "tls", "mountPath": "/tls", "readOnly": True},
                        ],
                        "resources": {
                            "requests": {"cpu": "500m", "memory": "512Mi"},
                            "limits": {"memory": "2Gi"},
                        },
                    }
                ],
                "volumes": volumes,
            },
        },
    },
)
service(load_name, {"app": load_name}, [{"port": 8080}], True)
(directory / "gke.json").touch(mode=0o600)
(directory / "gke.json").write_text(
    json.dumps({"apiVersion": "v1", "kind": "List", "items": items})
)
print(directory / "gke.json")
