FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags "-s -w" -o /out/azure-node-termination-handler ./cmd/azure-node-termination-handler
# Task 11 adds cmd/fake-imds; build it here once it exists.

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/azure-node-termination-handler /azure-node-termination-handler
USER nonroot:nonroot
ENTRYPOINT ["/azure-node-termination-handler"]
