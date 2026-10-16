FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/inventory_app ./cmd/app/main.go

FROM alpine:3.20

RUN adduser -D -u 10001 appuser

WORKDIR /app
COPY --from=builder /app/inventory_app /app/inventory_app
COPY migrations /app/migrations

USER appuser

CMD ["/app/inventory_app"]
