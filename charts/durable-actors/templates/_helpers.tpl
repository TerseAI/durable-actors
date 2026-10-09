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
{{- if and .Values.usage.pubsubTopic .Values.usage.url }}{{ fail "configure one usage destination: HTTP or Pub/Sub" }}{{ end -}}
{{- if and .Values.usage.authorizationUrl (not (or .Values.usage.pubsubTopic .Values.usage.url)) }}{{ fail "usage admission requires metering" }}{{ end -}}
{{- if and (or .Values.usage.url .Values.usage.authorizationUrl) (or (empty .Values.usage.tokenSecret) (empty .Values.usage.tokenKey)) }}{{ fail "HTTP usage and admission require tokenSecret and tokenKey" }}{{ end -}}
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
