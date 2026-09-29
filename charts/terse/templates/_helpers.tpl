{{- define "terse.name" -}}
{{- printf "%s-terse" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "terse.image" -}}
{{- printf "%s@sha256:%s" .Values.image.repository (required "image.digest must be the published runtime sha256 digest" .Values.image.digest) -}}
{{- end -}}
{{- define "terse.validate" -}}
{{- if and .Values.gateway.enabled (eq (empty .Values.gateway.tlsSecret) (empty .Values.gateway.preSharedCert)) }}{{ fail "gateway requires exactly one of tlsSecret or preSharedCert" }}{{ end -}}
{{- if eq .Values.sandboxNamespace .Release.Namespace }}{{ fail "sandboxNamespace must differ from the control-plane namespace" }}{{ end -}}
{{- if not (hasKey .Values.zones .Values.region) }}{{ fail "region must have a configured placement zone" }}{{ end -}}
{{- $zones := dict -}}{{- $regions := dict -}}
{{- range .Values.storage.replicas.placements -}}
{{- $_ := set $zones . true -}}{{- $_ := set $regions (regexReplaceAll "-[a-z]$" . "") true -}}
{{- end -}}
{{- if not .Values.storage.replicas.placements }}{{ fail "at least one replica is required" }}{{ end -}}
{{- if and (eq .Values.storage.durability "zonal") (ne (len $zones) 1) }}{{ fail "zonal persistence requires replicas in one zone" }}{{ end -}}
{{- if and (eq .Values.storage.durability "regional") (or (lt (len $zones) 2) (ne (len $regions) 1)) }}{{ fail "regional persistence requires multiple zones in one region" }}{{ end -}}
{{- if and (eq .Values.storage.durability "multi_region") (lt (len $regions) 2) }}{{ fail "multi_region persistence requires multiple regions" }}{{ end -}}
{{- if and .Values.storage.replicas.addresses (ne (len .Values.storage.replicas.addresses) (len .Values.storage.replicas.placements)) }}{{ fail "replica addresses must match placements" }}{{ end -}}
{{- range .Values.storage.replicas.deployIndices -}}
{{- if ge (int .) (len $.Values.storage.replicas.placements) }}{{ fail "replica deployIndices must index configured placements" }}{{ end -}}
{{- end -}}
{{- end -}}
{{- define "terse.replicas" -}}
{{- $members := list -}}
{{- range $i, $zone := .Values.storage.replicas.placements -}}
{{- $id := printf "%s-r%d" (include "terse.name" $) $i -}}
{{- $address := printf "http://%s.%s.svc.cluster.local:7200" $id $.Release.Namespace -}}
{{- if $.Values.storage.replicas.addresses }}{{ $address = index $.Values.storage.replicas.addresses $i }}{{ end -}}
{{- $members = append $members (dict "id" $id "address" $address "zone" $zone) -}}
{{- end -}}
{{- $members | toJson -}}
{{- end -}}
