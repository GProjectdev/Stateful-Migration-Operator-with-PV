#!/usr/bin/env bash
set -euo pipefail
umask 077
: "${TARGET_KUBECONFIG:?Set the explicit onpre2 member kubeconfig path}"
: "${IMAGE:?Set the image built from this directory and pushed to your registry}"
WORKLOAD_NAMESPACE="${WORKLOAD_NAMESPACE:-migration-demo}"
[[ -r "$TARGET_KUBECONFIG" ]] || { echo 'Target kubeconfig is not readable' >&2; exit 1; }
[[ "$WORKLOAD_NAMESPACE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ && ${#WORKLOAD_NAMESPACE} -le 63 ]] || exit 1
[[ "$IMAGE" =~ ^[a-zA-Z0-9][a-zA-Z0-9._:/@-]*$ ]] || { echo 'Invalid image reference' >&2; exit 1; }
for tool in kubectl openssl mktemp base64 tr; do command -v "$tool" >/dev/null; done
k() { kubectl --kubeconfig="$TARGET_KUBECONFIG" "$@"; }
k get crd checkpointrestores.migration.dcnlab.com >/dev/null
k get nodes
workdir="$(mktemp -d)"
trap 'rm -rf -- "$workdir"' EXIT
openssl genrsa -out "$workdir/ca.key" 2048
openssl req -x509 -new -key "$workdir/ca.key" -sha256 -days 365 \
  -subj /CN=legacy-checkpoint-restore-ca -out "$workdir/ca.crt"
openssl genrsa -out "$workdir/tls.key" 2048
openssl req -new -key "$workdir/tls.key" \
  -subj /CN=checkpoint-restore-webhook-svc.stateful-migration.svc -out "$workdir/tls.csr"
cat > "$workdir/extensions.cnf" <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:checkpoint-restore-webhook-svc,DNS:checkpoint-restore-webhook-svc.stateful-migration,DNS:checkpoint-restore-webhook-svc.stateful-migration.svc,DNS:checkpoint-restore-webhook-svc.stateful-migration.svc.cluster.local
EOF
openssl x509 -req -in "$workdir/tls.csr" -CA "$workdir/ca.crt" -CAkey "$workdir/ca.key" \
  -CAcreateserial -out "$workdir/tls.crt" -days 365 -sha256 -extfile "$workdir/extensions.cnf"
openssl verify -CAfile "$workdir/ca.crt" "$workdir/tls.crt"
ca_bundle="$(base64 < "$workdir/ca.crt" | tr -d '\r\n')"
cert_hash="$(openssl x509 -in "$workdir/tls.crt" -noout -fingerprint -sha256 | tr -d ':')"
for ns in stateful-migration "$WORKLOAD_NAMESPACE"; do
  k create namespace "$ns" --dry-run=client -o yaml | k apply -f -
done
k -n stateful-migration create secret tls checkpoint-restore-webhook-tls \
  --cert="$workdir/tls.crt" --key="$workdir/tls.key" --dry-run=client -o yaml | k apply -f -
cat > "$workdir/server.yaml" <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: checkpoint-restore-webhook
  namespace: stateful-migration
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: checkpoint-restore-webhook
  namespace: ${WORKLOAD_NAMESPACE}
rules:
- apiGroups: [migration.dcnlab.com]
  resources: [checkpointrestores]
  verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: checkpoint-restore-webhook
  namespace: ${WORKLOAD_NAMESPACE}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: checkpoint-restore-webhook
subjects:
- kind: ServiceAccount
  name: checkpoint-restore-webhook
  namespace: stateful-migration
---
apiVersion: v1
kind: Service
metadata:
  name: checkpoint-restore-webhook-svc
  namespace: stateful-migration
spec:
  selector: {app: checkpoint-restore-webhook}
  ports:
  - name: https
    port: 443
    targetPort: 8443
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: checkpoint-restore-webhook
  namespace: stateful-migration
spec:
  replicas: 1
  selector:
    matchLabels: {app: checkpoint-restore-webhook}
  template:
    metadata:
      labels: {app: checkpoint-restore-webhook}
      annotations:
        legacy-demo/tls-fingerprint: "${cert_hash}"
    spec:
      serviceAccountName: checkpoint-restore-webhook
      securityContext:
        runAsNonRoot: true
        runAsUser: 65532
        fsGroup: 65532
        seccompProfile: {type: RuntimeDefault}
      containers:
      - name: webhook
        image: "${IMAGE}"
        imagePullPolicy: Always
        ports: [{name: https, containerPort: 8443}]
        readinessProbe:
          httpGet: {scheme: HTTPS, path: /readyz, port: https}
        livenessProbe:
          httpGet: {scheme: HTTPS, path: /healthz, port: https}
        securityContext:
          allowPrivilegeEscalation: false
          readOnlyRootFilesystem: true
          capabilities: {drop: [ALL]}
        resources:
          requests: {cpu: 50m, memory: 64Mi}
          limits: {memory: 256Mi}
        volumeMounts:
        - name: tls
          mountPath: /tls
          readOnly: true
      volumes:
      - name: tls
        secret:
          secretName: checkpoint-restore-webhook-tls
EOF
k apply -f "$workdir/server.yaml"
k -n stateful-migration rollout status deployment/checkpoint-restore-webhook --timeout=180s
cat > "$workdir/admission.yaml" <<EOF
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: checkpoint-restore-webhook
webhooks:
- name: image-mutate.checkpoint-restore.k8s
  admissionReviewVersions: [v1]
  sideEffects: None
  timeoutSeconds: 5
  failurePolicy: Fail
  matchPolicy: Equivalent
  namespaceSelector:
    matchLabels:
      legacy-restore: enabled
  clientConfig:
    service:
      name: checkpoint-restore-webhook-svc
      namespace: stateful-migration
      path: /mutate
      port: 443
    caBundle: ${ca_bundle}
  rules:
  - operations: [CREATE]
    apiGroups: [""]
    apiVersions: [v1]
    resources: [pods]
    scope: Namespaced
EOF
k apply -f "$workdir/admission.yaml"
k label namespace "$WORKLOAD_NAMESPACE" legacy-restore=enabled --overwrite
echo "Installed legacy webhook for namespace $WORKLOAD_NAMESPACE. Verify member CheckpointRestore before releasing workload dispatch."
