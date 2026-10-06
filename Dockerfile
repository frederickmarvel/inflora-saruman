# syntax=docker/dockerfile:1.6
FROM golang:1.24-alpine AS builder
WORKDIR /src/inflora-saruman
COPY inflora-shared /src/inflora-shared
COPY inflora-saruman/go.mod inflora-saruman/go.sum ./
RUN go mod download
COPY inflora-saruman .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-saruman ./cmd/server
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/inflora-migrate ./cmd/migrate

FROM alpine:3.21
RUN apk add --no-cache ca-certificates wget && addgroup -S inflora && adduser -S -G inflora inflora
COPY --from=builder /out/inflora-saruman /usr/local/bin/inflora-saruman
COPY --from=builder /out/inflora-migrate /usr/local/bin/inflora-migrate
USER inflora:inflora
ENTRYPOINT ["/usr/local/bin/inflora-saruman"]
EXPOSE 7002
EXPOSE 8082
