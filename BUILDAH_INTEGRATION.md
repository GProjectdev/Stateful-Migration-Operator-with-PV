# Buildah Integration in CheckpointBackup Controller

## Overview

The CheckpointBackup controller now includes **buildah** and all necessary container tools built into the container image. This eliminates the need to install buildah on cluster nodes and ensures consistent container image creation capabilities.

## What Changed

### 🏗️ **Container Image Architecture**

#### CheckpointBackup Controller (Alpine-based)
- **Base Image**: Alpine Linux 3.19
- **Included Tools**:
  - `buildah` - Container image building tool
  - `fuse-overlayfs` - Overlay filesystem support
  - `crun` - Container runtime
  - `shadow` - User management utilities
  - `ca-certificates` - SSL/TLS certificates
  - `curl` - HTTP client for registry operations

#### MigrationBackup Controller (Distroless)
- **Base Image**: `gcr.io/distroless/static:nonroot`
- **Purpose**: Minimal image for Karmada control plane operations
- **Size**: Smaller footprint for management operations

### 🔧 **Security Configuration**

#### Non-Root User Setup
```dockerfile
# Create dedicated user for buildah operations
RUN addgroup -g 65532 -S nonroot && \
    adduser -u 65532 -S nonroot -G nonroot -h /home/nonroot
```

#### Buildah Storage Configuration
```dockerfile
# Configure isolated storage for buildah
RUN mkdir -p /home/nonroot/.config/containers && \
    echo '[storage]' > /home/nonroot/.config/containers/storage.conf && \
    echo 'driver = "overlay"' >> /home/nonroot/.config/containers/storage.conf
```

### 📁 **Volume Mounts**

The DaemonSet now includes additional storage for buildah user data:

| Mount Point | Host Path | Purpose |
|-------------|-----------|---------|
| `/var/lib/kubelet/checkpoints` | `/var/lib/kubelet/checkpoints` | Kubelet checkpoint files |
| `/var/lib/containers` | `/var/lib/containers` | System container storage |
| `/home/nonroot/.local/share/containers` | `/var/lib/buildah-user-storage` | User container storage |
| `/etc/containers` | `/etc/containers` | Container configuration |
| `/run/containers` | `/run/containers` | Runtime data |

### 🚀 **Build Process**

The build script now creates different Dockerfiles based on controller type:

#### For CheckpointBackup Controller
```bash
./build-and-push.sh checkpoint v1.17
```
Creates: `lehuannhatrang/stateful-migration-operator:checkpointBackup_v1.17`
- **Size**: ~100-150MB (includes buildah tools)
- **Capabilities**: Full container image building

#### For MigrationBackup Controller  
```bash
./build-and-push.sh migration v1.17
```
Creates: `lehuannhatrang/stateful-migration-operator:migrationBackup_v1.17`
- **Size**: ~10-20MB (minimal distroless)
- **Capabilities**: Management operations only

## Benefits

### ✅ **Simplified Node Requirements**
- **Before**: Nodes needed buildah installed
- **After**: Buildah included in container image

### ✅ **Consistent Environment**
- **Before**: Different buildah versions across nodes
- **After**: Same buildah version in all containers

### ✅ **Improved Security**
- **Before**: Buildah running as root on host
- **After**: Buildah running as non-root user in container

### ✅ **Easier Deployment**
- **Before**: Manual buildah installation on each node
- **After**: Single container deployment

## Container Operations

### Building Checkpoint Images
```go
// The controller now runs buildah commands inside the container
cmd := exec.Command("buildah", "from", "scratch")
out, err := cmd.Output()
```

### Registry Authentication
```go
// Registry login using container-internal buildah
cmd := exec.Command("buildah", "login", "-u", username, "-p", password, registry)
```

### Image Creation Flow
1. **Checkpoint Creation**: Kubelet API call creates checkpoint file
2. **Image Building**: Buildah creates container image from checkpoint
3. **Registry Push**: Buildah pushes image to configured registry

## Troubleshooting

### Debug Buildah Inside Container
```bash
# Check buildah version
kubectl exec -n stateful-migration <pod-name> -- buildah version

# Check storage configuration  
kubectl exec -n stateful-migration <pod-name> -- buildah info

# Test registry connectivity
kubectl exec -n stateful-migration <pod-name> -- buildah login your-registry.com
```

### Common Issues

#### Storage Permissions
```bash
# Check volume mount permissions
kubectl exec -n stateful-migration <pod-name> -- ls -la /home/nonroot/.local/share/containers
```

#### Overlay Filesystem
```bash
# Verify fuse-overlayfs is working
kubectl exec -n stateful-migration <pod-name> -- fuse-overlayfs --version
```

## Performance Impact

### Image Size Comparison
| Controller Type | Image Size | Base Image |
|----------------|------------|------------|
| CheckpointBackup | ~120MB | Alpine 3.19 + buildah |
| MigrationBackup | ~15MB | Distroless static |

### Storage Requirements
- **Buildah storage**: ~1-10GB per node (for image cache)
- **Checkpoint files**: ~50-500MB per container
- **User storage**: ~100MB-1GB per node

## Deployment

The buildah integration is automatically included when deploying:

```bash
# Deploy with buildah support
kubectl apply -f config/checkpoint-backup/daemonset.yaml
```

The DaemonSet will automatically:
1. Pull the buildah-enabled container image
2. Mount required storage volumes
3. Configure buildah user environment
4. Start checkpoint operations

## Security Considerations

### Container Privileges
- **Privileged**: Required for container operations
- **Capabilities**: `SYS_ADMIN`, `SYS_PTRACE` for checkpoint/restore
- **User**: Non-root (`nonroot:nonroot` 65532:65532)

### Network Access
- **Host Network**: Required for kubelet API access
- **Registry Access**: HTTPS connections to container registry
- **Internal Storage**: Isolated user storage directory

This integration provides a complete, self-contained solution for checkpoint-based stateful workload migration! 🎉 