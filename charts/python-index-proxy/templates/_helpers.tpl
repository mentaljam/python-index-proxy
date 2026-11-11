{{- define "python-index-proxy.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "python-index-proxy.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name (include "python-index-proxy.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "python-index-proxy.selectorLabels" -}}
app.kubernetes.io/name: {{ include "python-index-proxy.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "python-index-proxy.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | quote }}
{{ include "python-index-proxy.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "python-index-proxy.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "python-index-proxy.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "python-index-proxy.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}
{{- end -}}

{{- define "python-index-proxy.prefix" -}}
{{- printf "%s/" (trimSuffix "/" .Values.config.path_prefix) -}}
{{- end -}}

{{- define "python-index-proxy.config" -}}
{{- $config := deepCopy .Values.config -}}
{{- $_ := set $config "bind" (printf "0.0.0.0:%v" .Values.containerPort) -}}
{{- $wrapped := dict "value" $config -}}
{{- include "python-index-proxy.normalizeNumbers" $wrapped -}}
{{- $wrapped.value | toToml -}}
{{- end -}}

{{/* Helm can load integer values as float64; TOML must keep integer fields integral. */}}
{{- define "python-index-proxy.normalizeNumbers" -}}
{{- $value := .value -}}
{{- if kindIs "map" $value -}}
{{- range $key, $child := $value -}}
{{- $wrapped := dict "value" $child -}}
{{- include "python-index-proxy.normalizeNumbers" $wrapped -}}
{{- $_ := set $value $key $wrapped.value -}}
{{- end -}}
{{- else if kindIs "slice" $value -}}
{{- $items := list -}}
{{- range $child := $value -}}
{{- $wrapped := dict "value" $child -}}
{{- include "python-index-proxy.normalizeNumbers" $wrapped -}}
{{- $items = append $items $wrapped.value -}}
{{- end -}}
{{- $_ := set . "value" $items -}}
{{- else if kindIs "float64" $value -}}
{{- if eq $value (float64 (int64 $value)) -}}
{{- $_ := set . "value" (int64 $value) -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "python-index-proxy.validate" -}}
{{- if and .Values.ingress.enabled .Values.httpRoute.enabled -}}
{{- fail "enable either ingress or httpRoute, not both" -}}
{{- end -}}
{{- $fs := hasKey .Values.config.storage "fs" -}}
{{- if and .Values.persistence.enabled (not $fs) -}}
{{- fail "persistence is only supported with config.storage.fs" -}}
{{- end -}}
{{- if $fs -}}
{{- $mount := trimSuffix "/" .Values.persistence.mountPath -}}
{{- if or (regexMatch "(^|/)\\.\\.?(/|$)" $mount) (hasPrefix "/etc/python-index-proxy" $mount) -}}
{{- fail "persistence.mountPath must be a canonical path outside /etc/python-index-proxy" -}}
{{- end -}}
{{- range $field := list "root" "atomic_write_dir" -}}
{{- $path := get $.Values.config.storage.fs $field -}}
{{- if and $path (or (regexMatch "(^|/)\\.\\.?(/|$)" $path) (contains "//" $path)) -}}
{{- fail (printf "config.storage.fs.%s must be a canonical path" $field) -}}
{{- end -}}
{{- if and $path (not (or (eq $path $mount) (hasPrefix (printf "%s/" $mount) $path))) -}}
{{- fail (printf "config.storage.fs.%s must be inside persistence.mountPath" $field) -}}
{{- end -}}
{{- end -}}
{{- if and (gt (int .Values.replicaCount) 1) (or (not .Values.persistence.enabled) (not (has "ReadWriteMany" .Values.persistence.accessModes))) -}}
{{- fail "multiple filesystem replicas require persistence.enabled and ReadWriteMany accessModes" -}}
{{- end -}}
{{- end -}}
{{- if and .Values.persistence.existingClaim (not .Values.persistence.enabled) -}}
{{- fail "persistence.existingClaim requires persistence.enabled" -}}
{{- end -}}
{{- $envNames := dict -}}
{{- range .Values.env -}}
{{- if hasKey $envNames .name -}}
{{- fail (printf "duplicate env variable %s" .name) -}}
{{- end -}}
{{- $_ := set $envNames .name true -}}
{{- if eq .name "PIPR__BIND" -}}
{{- fail "PIPR__BIND is managed by containerPort; do not set it in env" -}}
{{- end -}}
{{- end -}}
{{- range $key, $_ := .Values.podLabels -}}
{{- if has $key (list "app.kubernetes.io/name" "app.kubernetes.io/instance") -}}
{{- fail (printf "podLabels must not override selector label %s" $key) -}}
{{- end -}}
{{- end -}}
{{- end -}}
