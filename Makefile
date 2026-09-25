IMG ?= ghcr.io/gprojectdev/stateful-migration-system:dev
.PHONY: test vet build docker-build docker-push manifests
test:
	go test ./...
vet:
	go vet ./...
build:
	go build -o bin/manager ./cmd/manager
docker-build:
	docker build -t $(IMG) .
docker-push:
	docker push $(IMG)
manifests:
	kustomize build config/crd
	kustomize build config/karmada
	kustomize build config/management
	kustomize build config/member
	kustomize build config/checkpoint
