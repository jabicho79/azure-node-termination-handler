.PHONY: build test vet docker-build helm-lint e2e
build:
	go build ./...
test:
	go test ./...
vet:
	go vet ./...
docker-build:
	@echo TODO
helm-lint:
	@echo TODO
e2e:
	@echo TODO
