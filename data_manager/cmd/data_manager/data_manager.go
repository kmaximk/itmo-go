package main

import (
	"data_manager/internal/api"
	"data_manager/internal/repository"
	"data_manager/pkg/generated/proto/data_manager"
	"data_manager/pkg/generated/proto/order_service"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

func main() {
	orderRepository := repository.NewInMemoryOrderRepository()

	var (
		orderServiceAddress string
		appPort             string
	)

	flag.StringVar(&orderServiceAddress, "order_address", "localhost:8094", "address of data manager")
	flag.StringVar(&appPort, "app_port", "8093", "application port")

	flag.Parse()

	orderServiceConnect, err := grpc.Dial(orderServiceAddress, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}...)

	if err != nil {
		log.Fatal(err)
	}

	orderServiceClient := order_service.NewOrderServiceClient(orderServiceConnect)

	dataManagerApi := api.NewDataManagerApi(orderRepository, orderServiceClient)

	lsn, err := net.Listen("tcp", fmt.Sprintf(":%s", appPort))

	if err != nil {
		log.Fatal(err)
	}

	var dataManagerServer = grpc.NewServer()

	reflection.Register(dataManagerServer)
	data_manager.RegisterDataManagerServer(dataManagerServer, dataManagerApi)

	if err := dataManagerServer.Serve(lsn); err != nil {
		log.Fatalf("serving error %v", err)
	}
}
