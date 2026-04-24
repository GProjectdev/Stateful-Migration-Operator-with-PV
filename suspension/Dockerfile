FROM golang:1.24 AS builder
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go build -o /out/suspension-controller ./suspension/cmd

FROM gcr.io/distroless/static:nonroot
COPY --from=builder /out/suspension-controller /suspension-controller
USER 65532:65532
ENTRYPOINT ["/suspension-controller"]