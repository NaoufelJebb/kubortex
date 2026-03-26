{{/*
Expand the name of the chart.
*/}}
{{- define "kubortex.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kubortex.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label (name + version).
*/}}
{{- define "kubortex.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "kubortex.labels" -}}
helm.sh/chart: {{ include "kubortex.chart" . }}
app.kubernetes.io/name: {{ include "kubortex.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/part-of: kubortex
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for a given component.
Usage: include "kubortex.selectorLabels" (list . "operator")
*/}}
{{- define "kubortex.selectorLabels" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
app.kubernetes.io/name: {{ printf "kubortex-%s" $component }}
app.kubernetes.io/instance: {{ $ctx.Release.Name }}
app.kubernetes.io/component: {{ $component }}
{{- end }}

{{/*
Component labels (common + selector).
Usage: include "kubortex.componentLabels" (list . "operator")
*/}}
{{- define "kubortex.componentLabels" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
{{ include "kubortex.labels" $ctx }}
app.kubernetes.io/component: {{ $component }}
{{- end }}

{{/*
Name of the LLM secret — uses existingSecret if set, otherwise the chart-managed secret.
*/}}
{{- define "kubortex.llmSecretName" -}}
{{- if .Values.llm.existingSecret }}
{{- .Values.llm.existingSecret }}
{{- else }}
{{- printf "%s-llm" (include "kubortex.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Name of the Slack secret.
*/}}
{{- define "kubortex.slackSecretName" -}}
{{- if .Values.slack.existingSecret }}
{{- .Values.slack.existingSecret }}
{{- else }}
{{- printf "%s-slack" (include "kubortex.fullname" .) }}
{{- end }}
{{- end }}

{{/*
Standard environment variables shared by all components.
*/}}
{{- define "kubortex.commonEnv" -}}
- name: KUBORTEX_NAMESPACE
  valueFrom:
    fieldRef:
      fieldPath: metadata.namespace
- name: KUBORTEX_PROMETHEUS_URL
  value: {{ .Values.prometheus.url | quote }}
- name: KUBORTEX_LOKI_URL
  value: {{ .Values.loki.url | quote }}
- name: KUBORTEX_INVESTIGATOR_MAX_ITERATIONS
  value: {{ .Values.kubortex.investigatorMaxIterations | quote }}
- name: KUBORTEX_INVESTIGATOR_TIMEOUT_SECONDS
  value: {{ .Values.kubortex.investigatorTimeoutSeconds | quote }}
- name: KUBORTEX_CONTEXT_MAX_CHARS
  value: {{ .Values.kubortex.contextMaxChars | quote }}
{{- end }}
