FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -trimpath -ldflags "-s -w" -o /out/azure-node-termination-handler ./cmd/azure-node-termination-handler
RUN go build -trimpath -ldflags "-s -w" -o /out/fake-imds ./cmd/fake-imds

FROM gcr.io/distroless/static:nonroot
COPY --from=build /out/azure-node-termination-handler /azure-node-termination-handler
COPY --from=build /out/fake-imds /fake-imds
USER nonroot:nonroot
ENTRYPOINT ["/azure-node-termination-handler"]
