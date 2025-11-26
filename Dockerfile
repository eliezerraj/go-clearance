# docker build -t go-clearance .
# docker run -dit --name go-clearance -p 7003:7003 go-clearance

FROM golang:1.24 As builder

RUN apt-get update && apt-get install bash && apt-get install -y --no-install-recommends ca-certificates

WORKDIR /app
COPY . .
RUN go mod tidy

WORKDIR /app/cmd
RUN go build -o go-carclearancet -ldflags '-linkmode external -w -extldflags "-static"'

FROM alpine

WORKDIR /app
COPY --from=builder /app/cmd/go-clearance .
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["/app/go-clearance"]