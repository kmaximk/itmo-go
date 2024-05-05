FROM golang:1.21

WORKDIR /go/delivery

RUN apt update
RUN apt install unzip

COPY . .
RUN make build

CMD ["./bin/app", "-order_address", "host.docker.internal:8085", "-app_port", "8082"]
