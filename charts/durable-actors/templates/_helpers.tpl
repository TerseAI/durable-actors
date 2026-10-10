{{- define "terse.socketGatewayName" -}}
{{- printf "%s-sockets" (include "terse.name" . | trunc 55 | trimSuffix "-") -}}
{{- end -}}
{{- define "terse.name" -}}
{{- printf "%s-terse" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "terse.image" -}}
{{- printf "%s@sha256:%s" .repository (required "image digest is required for source builds; published charts include all image digests" .digest) -}}
{{- end -}}
{{- define "terse.validate" -}}
{{- if gt (int .Values.substrate.worker.autoscaling.minReplicas) (int .Values.substrate.worker.autoscaling.maxReplicas) }}{{ fail "worker autoscaling minReplicas must not exceed maxReplicas" }}{{ end -}}
{{- if and .Values.gateway.enabled (eq (empty .Values.gateway.tlsSecret) (empty .Values.gateway.preSharedCert)) }}{{ fail "gateway requires exactly one of tlsSecret or preSharedCert" }}{{ end -}}
{{- if lt (int .Values.replicaCount) 2 }}{{ fail "regional availability requires multiple control-plane replicas" }}{{ end -}}
{{- if eq .Values.substrate.namespace .Release.Namespace }}{{ fail "Substrate workers require a separate namespace" }}{{ end -}}
{{- if eq .Values.storage.mode "rapid" -}}
{{- $zones := dict -}}{{- $buckets := dict -}}
{{- if ne (len .Values.storage.rapid.buckets) 2 }}{{ fail "append logs require exactly two Rapid zones" }}{{ end -}}
{{- range .Values.storage.rapid.buckets -}}
{{- if hasKey $zones .zone }}{{ fail "Rapid buckets require distinct zones" }}{{ end -}}
{{- if or (hasKey $buckets .bucket) (eq .bucket $.Values.storage.archiveBucket) }}{{ fail "Rapid and Standard buckets must be distinct" }}{{ end -}}
{{- $_ := set $zones .zone true -}}{{- $_ := set $buckets .bucket true -}}
{{- end -}}
{{- else if .Values.storage.rapid.buckets }}{{ fail "Rapid buckets require storage.mode=rapid" }}{{ end -}}
{{- end -}}
