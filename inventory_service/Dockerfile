FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod ./
# If go.sum exists copy it, otherwise go mod tidy in build
COPY go.sum* ./
RUN go mod download || true

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/inventory_app ./cmd/app/main.go

FROM alpine:3.20

WORKDIR /app
COPY --from=builder /app/inventory_app /app/inventory_app

CMD ["/app/inventory_app"]
