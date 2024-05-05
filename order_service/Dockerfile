FROM golang:1.21

WORKDIR /go/delivery

RUN apt update
RUN apt install unzip

COPY . .
RUN make build

CMD ["./bin/app", "-data_manager_address", "host.docker.internal:8086", "-app_port", "8082"]
