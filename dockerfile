FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o nutelladb .

FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

COPY --from=builder /app/nutelladb .

RUN mkdir -p /root/files

EXPOSE 3000

ENTRYPOINT ["./nutelladb"]

CMD ["help"]