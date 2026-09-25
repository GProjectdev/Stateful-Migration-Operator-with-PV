#!/usr/bin/env bash
set -euo pipefail
umask 077
context="${1:-karmada}"
output="${2:-/tmp/stateful-karmada.kubeconfig}"
if [[ -e "$output" ]]; then
  echo "Refusing to overwrite existing kubeconfig: $output" >&2
  exit 1
fi
server="${KARMADA_SERVER:-$(kubectl --context "$context" config view --minify --raw -o jsonpath='{.clusters[0].cluster.server}')}"
ca="$(kubectl --context "$context" config view --minify --raw --flatten -o jsonpath='{.clusters[0].cluster.certificate-authority-data}')"
if [[ -z "$server" || -z "$ca" ]]; then
  echo "A reachable Karmada server and trusted CA data are required." >&2
  exit 1
fi
token="$(kubectl --context "$context" -n stateful-migration-system create token stateful-management --duration=1h)"
kubectl --kubeconfig "$output" config set-cluster karmada --server="$server" >/dev/null
kubectl --kubeconfig "$output" config set clusters.karmada.certificate-authority-data "$ca" >/dev/null
kubectl --kubeconfig "$output" config set-credentials stateful-management --token="$token" >/dev/null
kubectl --kubeconfig "$output" config set-context stateful-management --cluster=karmada --user=stateful-management >/dev/null
kubectl --kubeconfig "$output" config use-context stateful-management >/dev/null
unset token
echo "Created $output. Token lifetime requested: 1 hour; rotate the Secret before expiry."
