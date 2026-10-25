FROM golang:1.26-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/gateway ./gateway

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -g 65532 -S app && adduser -u 65532 -S -G app app

COPY --from=build /out/gateway /gateway
USER app:app
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O- http://127.0.0.1:8080/health >/dev/null || exit 1
ENTRYPOINT ["/gateway"]
