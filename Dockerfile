FROM golang:1.23-alpine AS builder

WORKDIR /src

COPY go.mod ./
COPY main.go ./

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/event-service .

FROM alpine:3.22

RUN apk --no-cache add ca-certificates \
    && adduser -D -H -u 10001 appuser

COPY --from=builder /out/event-service /event-service

USER appuser

EXPOSE 8080

ENTRYPOINT ["/event-service"]