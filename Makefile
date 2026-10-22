.PHONY: build test vet docker-build helm-lint e2e
build:
	go build ./...
test:
	go test ./...
vet:
	go vet ./...
IMAGE ?= azure-node-termination-handler:dev
docker-build:
	docker build -t $(IMAGE) .
helm-lint:
	helm lint charts/azure-node-termination-handler
e2e:
	@echo TODO
