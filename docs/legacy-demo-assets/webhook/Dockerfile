FROM docker.io/library/golang:1.24.4 AS builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -mod=readonly -trimpath -o /webhook-server ./main.go
FROM gcr.io/distroless/static:nonroot
COPY --from=builder /webhook-server /webhook-server
USER 65532:65532
ENTRYPOINT ["/webhook-server"]
