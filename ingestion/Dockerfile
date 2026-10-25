FROM golang:1.26-alpine AS builder
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o ingestion ./ingestion

FROM alpine:3.20
RUN apk add --no-cache ca-certificates wget && addgroup -S app && adduser -S app -G app
WORKDIR /app
COPY --from=builder /app/ingestion .
USER app
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --retries=3 CMD wget -qO- http://localhost:3000/health || exit 1
CMD ["./ingestion"]
