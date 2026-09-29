{{- define "terse.name" -}}
{{- printf "%s-terse" .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- define "terse.image" -}}
{{- printf "%s@sha256:%s" .Values.image.repository (required "image.digest must be the published runtime sha256 digest" .Values.image.digest) -}}
{{- end -}}
{{- define "terse.validate" -}}
{{- if eq .Values.sandboxNamespace .Release.Namespace }}{{ fail "sandboxNamespace must differ from the control-plane namespace" }}{{ end -}}
{{- if not (hasKey .Values.zones .Values.region) }}{{ fail "region must have a configured placement zone" }}{{ end -}}
{{- if not .Values.storage.rapidBuckets }}{{ fail "at least one Rapid bucket is required" }}{{ end -}}
{{- $zones := dict -}}{{- $regions := dict -}}{{- $names := dict -}}
{{- range .Values.storage.rapidBuckets -}}
{{- if hasKey $names .name }}{{ fail "Rapid bucket names must be unique" }}{{ end -}}
{{- $_ := set $names .name true -}}{{- $_ := set $zones .zone true -}}
{{- $_ := set $regions (regexReplaceAll "-[a-z]$" .zone "") true -}}
{{- end -}}
{{- if and (eq .Values.storage.durability "zonal") (ne (len $zones) 1) }}{{ fail "zonal persistence requires buckets in one zone" }}{{ end -}}
{{- if and (eq .Values.storage.durability "regional") (or (lt (len $zones) 2) (ne (len $regions) 1)) }}{{ fail "regional persistence requires multiple zones in one region" }}{{ end -}}
{{- if and (eq .Values.storage.durability "multi_region") (lt (len $regions) 2) }}{{ fail "multi_region persistence requires multiple regions" }}{{ end -}}
{{- range $region, $zone := .Values.zones -}}
{{- if not (hasKey $zones $zone) }}{{ fail "each compute zone must have a local Rapid bucket" }}{{ end -}}
{{- end -}}
{{- end -}}
