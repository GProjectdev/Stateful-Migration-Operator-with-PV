# Build Script Usage Guide

The `build-and-push.sh` script has been updated to support building different controller variants for the Stateful Migration Operator.

## Quick Start

### Build All Controllers
```bash
./build-and-push.sh
# or
./build-and-push.sh all
# or with custom version
./build-and-push.sh all v1.17
```

### Build Only CheckpointBackup Controller
```bash
./build-and-push.sh checkpoint
# or with custom version
./build-and-push.sh checkpoint v2.0
```

### Build Only MigrationBackup Controller
```bash
./build-and-push.sh migration
# or with custom version
./build-and-push.sh migration v1.18
```

## Built Images

The script will create the following Docker images (default version v1.16):

1. **CheckpointBackup Controller** (for DaemonSet deployment on member clusters):
   ```
   lehuannhatrang/stateful-migration-operator:checkpointBackup_<VERSION>
   ```

2. **MigrationBackup Controller** (for Karmada control plane):
   ```
   lehuannhatrang/stateful-migration-operator:migrationBackup_<VERSION>
   ```

**Examples with different versions:**
- `./build-and-push.sh all v1.17` creates:
  - `lehuannhatrang/stateful-migration-operator:checkpointBackup_v1.17`
  - `lehuannhatrang/stateful-migration-operator:migrationBackup_v1.17`

## Controller Configurations

### CheckpointBackup Controller
- **Purpose**: Runs as DaemonSet on member cluster nodes
- **Enabled Controllers**: CheckpointBackup only
- **Default Flags**:
  - `--enable-checkpoint-backup-controller=true`
  - `--enable-migration-backup-controller=false`
  - `--enable-migration-restore-controller=false`

### MigrationBackup Controller
- **Purpose**: Runs on Karmada control plane
- **Enabled Controllers**: MigrationBackup + MigrationRestore
- **Default Flags**:
  - `--enable-checkpoint-backup-controller=false`
  - `--enable-migration-backup-controller=true`
  - `--enable-migration-restore-controller=true`

## Deployment Examples

After building, the script creates deployment examples:

- `checkpoint-deploy-example.yaml`: DaemonSet configuration for CheckpointBackup controller
- `migration-deploy-example.yaml`: Deployment configuration for MigrationBackup controller

## Prerequisites

- Docker installed and running
- Docker Hub login: `docker login`
- Make command available
- Go 1.24+ (for building)

## What the Script Does

1. **Checks prerequisites** (Docker, login status, make command)
2. **Generates manifests and CRDs** (`make manifests generate`)
3. **Runs tests** (optional, can skip if failed)
4. **Creates controller-specific Dockerfiles** with appropriate flags
5. **Builds Docker images** for selected controllers
6. **Tests images locally** (basic smoke test)
7. **Pushes to Docker Hub** (`lehuannhatrang/stateful-migration-operator`)
8. **Creates deployment examples**
9. **Shows build summary** with image sizes and deployment instructions

## Help

```bash
./build-and-push.sh --help
```

## Troubleshooting

### Docker Login Issues
```bash
docker login
```

### Permission Issues
```bash
sudo ./build-and-push.sh
```

### Build Failures
Check Docker daemon is running:
```bash
docker info
```

Check Go modules:
```bash
go mod tidy
make manifests generate
``` 