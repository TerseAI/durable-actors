{{- define "actors.name" -}}
{{- printf "%s-durable-actors" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "actors.sandboxNamespace" -}}
{{- default (printf "%s-%s" .Release.Namespace .Release.Name | trunc 63 | trimSuffix "-") .Values.sandboxNamespace -}}
{{- end -}}
{{- define "actors.image" -}}
{{- printf "%s@sha256:%s" .Values.image.repository (required "image.digest is required for source builds; published charts include it" .Values.image.digest) -}}
{{- end -}}
{{- define "actors.validate" -}}
{{- if eq .Values.runtimeMode "hybrid" -}}
{{- if not (regexMatch "^gs://[^/]+/" .Values.substrate.snapshotLocation) }}{{ fail "hybrid mode requires substrate.snapshotLocation, a GCS prefix ending in /" }}{{ end -}}
{{- if not (hasSuffix "/" .Values.substrate.snapshotLocation) }}{{ fail "substrate.snapshotLocation must end in /" }}{{ end -}}
{{- if or (empty .Values.cluster.projectId) (empty .Values.cluster.location) (empty .Values.cluster.name) }}{{ fail "hybrid autoscaling requires cluster.projectId, cluster.location, and cluster.name" }}{{ end -}}
{{- if gt (int .Values.substrate.worker.autoscaling.minReplicas) (int .Values.substrate.worker.autoscaling.maxReplicas) }}{{ fail "worker autoscaling minReplicas must not exceed maxReplicas" }}{{ end -}}
{{- if or (eq .Values.substrate.namespace .Release.Namespace) (eq .Values.substrate.namespace (include "actors.sandboxNamespace" .)) }}{{ fail "Substrate workers require a separate namespace" }}{{ end -}}
{{- if ne (len .Values.substrate.regions) 1 }}{{ fail "hybrid mode requires one substrate.regions entry for this installation" }}{{ end -}}
{{- end -}}
{{- if eq (include "actors.sandboxNamespace" .) .Release.Namespace }}{{ fail "sandbox namespace must differ from the control-plane namespace" }}{{ end -}}
{{- if .Values.ingress.enabled -}}
{{- if or (empty .Values.ingress.className) (empty .Values.ingress.tlsSecret) }}{{ fail "ingress requires className and tlsSecret" }}{{ end -}}
{{- end -}}
{{- if eq .Values.storage.mode "rapid" -}}
{{- if ne (len .Values.storage.rapid.buckets) 2 }}{{ fail "Rapid requires exactly two buckets in distinct zones" }}{{ end -}}
{{- $zones := dict -}}{{- $buckets := dict -}}
{{- range .Values.storage.rapid.buckets -}}
{{- if hasKey $zones .zone }}{{ fail "Rapid buckets require distinct zones" }}{{ end -}}
{{- if or (hasKey $buckets .bucket) (eq .bucket $.Values.storage.bucket) }}{{ fail "Rapid and Standard buckets must be distinct" }}{{ end -}}
{{- $_ := set $zones .zone true -}}{{- $_ := set $buckets .bucket true -}}
{{- end -}}
{{- else if .Values.storage.rapid.buckets }}{{ fail "Rapid buckets require storage.mode=rapid" }}{{ end -}}
{{- end -}}
