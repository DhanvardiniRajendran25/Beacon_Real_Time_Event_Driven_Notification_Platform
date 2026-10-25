FROM golang:1.26-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG CGO_ENABLED=0
RUN go build -trimpath -ldflags="-s -w" -o /out/worker ./fanout/worker \
    && go build -trimpath -ldflags="-s -w" -o /out/kafka-producer ./fanout/test/kafka-producer \
    && go build -trimpath -ldflags="-s -w" -o /out/redis-subscriber ./fanout/test/redis-subscriber

FROM alpine:3.20 AS worker
RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -g 65532 -S app && adduser -u 65532 -S -G app app

COPY --from=build /out/worker /worker
USER app:app
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O- http://127.0.0.1:8080/health >/dev/null || exit 1
ENTRYPOINT ["/worker"]

FROM alpine:3.20 AS kafka-producer
RUN apk add --no-cache ca-certificates tzdata wget \
    && addgroup -g 65532 -S app && adduser -u 65532 -S -G app app

COPY --from=build /out/kafka-producer /kafka-producer
USER app:app
EXPOSE 8081
ENV HTTP_ADDR=:8081
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O- http://127.0.0.1:8081/health >/dev/null || exit 1
ENTRYPOINT ["/kafka-producer"]

FROM alpine:3.20 AS redis-subscriber
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -g 65532 -S app && adduser -u 65532 -S -G app app

COPY --from=build /out/redis-subscriber /redis-subscriber
USER app:app
ENTRYPOINT ["/redis-subscriber"]
