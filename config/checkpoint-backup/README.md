# CheckpointBackup Controller Deployment Guide

The CheckpointBackup controller runs as a DaemonSet on member clusters to handle checkpoint operations for stateful workloads.

## Overview

The CheckpointBackup controller:
- Runs as a DaemonSet on every node in member clusters
- Watches for CheckpointBackup CRs and processes only pods running on the same node
- Calls kubelet checkpoint API to create container checkpoints
- Uses built-in buildah to create checkpoint images
- Pushes checkpoint images to container registry
- Includes all necessary tools (buildah, fuse-overlayfs) in the container image

## Prerequisites

### 1. Karmada Multi-Cluster Setup
- Karmada control plane configured and running
- Member clusters registered with Karmada
- `kubectl` access to both Karmada control plane and member clusters

### 2. Node Requirements (on all member cluster nodes)
- Container runtime with checkpoint support (CRI-O or containerd with CRIU)
- Sufficient storage in `/var/lib/kubelet/checkpoints/`
- Sufficient storage in `/var/lib/containers/` and `/var/lib/buildah-user-storage/`
- Privileged container support enabled
- fuse-overlayfs support (usually available by default)

### 3. Registry Access
- Container registry for storing checkpoint images
- Registry credentials (username/password)

## Deployment Steps

### Step 1: Prepare Registry Credentials

1. Create registry credentials secret:
```bash
# Replace with your actual registry credentials
USERNAME="your-registry-username"
PASSWORD="your-registry-password"
REGISTRY="your-registry.com"  # Optional

# Create base64 encoded values
USERNAME_B64=$(echo -n "$USERNAME" | base64)
PASSWORD_B64=$(echo -n "$PASSWORD" | base64)
REGISTRY_B64=$(echo -n "$REGISTRY" | base64)

# Update the secret template
sed -i "s/<BASE64_ENCODED_USERNAME>/$USERNAME_B64/g" registry-credentials-secret.yaml
sed -i "s/<BASE64_ENCODED_PASSWORD>/$PASSWORD_B64/g" registry-credentials-secret.yaml
sed -i "s/<BASE64_ENCODED_REGISTRY_URL>/$REGISTRY_B64/g" registry-credentials-secret.yaml
```

2. Apply registry credentials to Karmada:
```bash
kubectl apply -f registry-credentials-secret.yaml --kubeconfig=karmada-config
```

### Step 2: Deploy CheckpointBackup Controller to Member Clusters

1. Create namespace on member clusters:
```bash
kubectl apply -f namespace.yaml --context=member-cluster-1
kubectl apply -f namespace.yaml --context=member-cluster-2
# Repeat for all member clusters
```

2. Apply RBAC resources:
```bash
kubectl apply -f ../rbac/checkpoint_backup_rbac.yaml --context=member-cluster-1
kubectl apply -f ../rbac/checkpoint_backup_rbac.yaml --context=member-cluster-2
# Repeat for all member clusters
```

3. Deploy the DaemonSet:
```bash
kubectl apply -f daemonset.yaml --context=member-cluster-1
kubectl apply -f daemonset.yaml --context=member-cluster-2
# Repeat for all member clusters
```

### Step 3: Update Registry Credentials PropagationPolicy

Update the PropagationPolicy in `registry-credentials-secret.yaml` to include your member cluster names:

```yaml
spec:
  placement:
    clusterAffinity:
      clusterNames:
      - member-cluster-1
      - member-cluster-2
      # Add all your member cluster names
```

Then apply the updated policy:
```bash
kubectl apply -f registry-credentials-secret.yaml --kubeconfig=karmada-config
```

### Step 4: Verify Deployment

1. Check DaemonSet status:
```bash
kubectl get daemonset checkpoint-backup-controller -n stateful-migration --context=member-cluster-1
```

2. Check pod logs:
```bash
kubectl logs -n stateful-migration -l app.kubernetes.io/name=checkpoint-backup-controller --context=member-cluster-1
```

3. Verify registry credentials are available:
```bash
kubectl get secret registry-credentials -n stateful-migration --context=member-cluster-1
```

## Configuration

### Environment Variables

The DaemonSet automatically configures these environment variables:
- `NODE_NAME`: Name of the Kubernetes node
- `NODE_IP`: IP address of the node
- `POD_NAME`: Name of the controller pod
- `POD_NAMESPACE`: Namespace of the controller pod

### Volume Mounts

Required volume mounts:
- `/var/lib/kubelet/checkpoints`: Kubelet checkpoint storage
- `/var/lib/containers`: Buildah storage
- `/etc/containers`: Container configuration
- `/run/containers`: Runtime data
- `/var/run`: System runtime data

### Security Context

The controller runs with privileged access and requires:
- `privileged: true`
- `SYS_ADMIN` and `SYS_PTRACE` capabilities
- `hostNetwork: true` and `hostPID: true`

## Troubleshooting

### Common Issues

1. **Checkpoint API calls fail**
   - Verify kubelet checkpoint API is enabled
   - Check service account token and RBAC permissions
   - Ensure node IP is correctly detected

2. **Buildah commands fail**
   - Check storage permissions and available space
   - Ensure privileged container permissions
   - Verify fuse-overlayfs is available on nodes

3. **Registry push fails**
   - Verify registry credentials are correct
   - Check network connectivity to registry
   - Ensure registry supports the image format

4. **Pod scheduling issues**
   - Check node tolerations and selectors
   - Verify DaemonSet deployment status
   - Review node resource availability

### Debug Commands

```bash
# Check controller logs
kubectl logs -n stateful-migration -l app.kubernetes.io/name=checkpoint-backup-controller --tail=100

# Verify service account permissions
kubectl auth can-i create pods/checkpoint --as=system:serviceaccount:stateful-migration:checkpoint-backup-sa

# Check buildah functionality (exec into controller pod)
kubectl exec -n stateful-migration <pod-name> -- buildah version

# Test registry connectivity
kubectl exec -n stateful-migration <pod-name> -- buildah login your-registry.com

# Check buildah storage configuration
kubectl exec -n stateful-migration <pod-name> -- buildah info
```

### Log Analysis

Look for these log patterns:
- `"Starting checkpoint operation"`: Normal checkpoint start
- `"Successfully checkpointed and pushed container image"`: Successful completion
- `"Pod is not on this node, skipping"`: Normal node filtering
- `"Failed to create checkpoint via kubelet API"`: Kubelet API issues
- `"Failed to build checkpoint image"`: Buildah issues
- `"Failed to push checkpoint image"`: Registry issues

## Performance Considerations

### Resource Requirements

Default resource requests/limits:
```yaml
resources:
  limits:
    cpu: 500m
    memory: 512Mi
  requests:
    cpu: 100m
    memory: 128Mi
```

Adjust based on:
- Number of pods per node
- Checkpoint frequency
- Image sizes
- Network bandwidth

### Storage Requirements

Ensure adequate storage for:
- Checkpoint files: `50MB - 500MB` per container
- Buildah storage: `1GB - 10GB` per node
- Image caches: `100MB - 1GB` per image

### Network Bandwidth

Consider registry upload bandwidth:
- Checkpoint images can be `50MB - 500MB`
- Multiple concurrent uploads per node
- Network congestion during scheduled checkpoints

## Security Considerations

### Privileged Access

The controller requires privileged access to:
- Access kubelet checkpoint API
- Mount host volumes
- Execute buildah commands

### Registry Security

- Use secure registry connections (HTTPS)
- Rotate registry credentials regularly
- Implement network policies if needed
- Consider private registry deployments

### Pod Security Standards

The DaemonSet requires:
- `privileged` security context
- Host network and PID access
- Multiple host volume mounts

Consider using Pod Security Standards with appropriate exemptions for this system component. 