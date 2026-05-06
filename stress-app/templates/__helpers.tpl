{{/*
 ====================================
 Standard Labels and Selectors
 ====================================
*/}}
{{- define "stress-app.labels" -}}
app: stress-app
release: {{ .Release.Name }}
chart: {{ .Chart.Name }}-{{ .Chart.Version }}
{{- end -}}