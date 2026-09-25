FROM golang:1.24 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api api
COPY internal internal
COPY cmd cmd
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /manager ./cmd/manager
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /manager /manager
ENTRYPOINT ["/manager"]
