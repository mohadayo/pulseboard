FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod ./
COPY *.go ./
RUN go build -o health-checker .

FROM alpine:3.20
RUN apk --no-cache add ca-certificates
WORKDIR /app
COPY --from=builder /app/health-checker .

EXPOSE 8002

CMD ["./health-checker"]
