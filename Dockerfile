FROM golang:1.22.2 AS builder

WORKDIR /app

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o main cmd/walrus3/main.go

FROM alpine:latest

RUN apk --no-cache add \
    ca-certificates \
    curl \
    wget \
    jq

WORKDIR /root/

COPY --from=builder /app/main .
RUN chmod +x main

ENTRYPOINT ["./main"]
