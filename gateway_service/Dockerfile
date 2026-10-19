FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/gateway_app ./cmd/gateway/main.go

FROM alpine:3.20

RUN adduser -D -u 10001 appuser

WORKDIR /app
COPY --from=builder /app/gateway_app /app/gateway_app

USER appuser

EXPOSE 8080

CMD ["/app/gateway_app"]
