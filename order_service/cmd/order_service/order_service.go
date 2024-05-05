package main

import (
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"order_service/internal/api"
	"order_service/pkg/generated/proto/data_manager"
	"order_service/pkg/generated/proto/order_service"
)

func main() {
	var (
		dataManagerAddress string
		appPort            string
	)

	flag.StringVar(&dataManagerAddress, "data_manager_address", "localhost:8093", "address of data manager")
	flag.StringVar(&appPort, "app_port", "8094", "application port")

	flag.Parse()
	dataManagerConnect, err := grpc.Dial(dataManagerAddress, []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}...)

	if err != nil {
		log.Fatal(err)
	}
	dataManagerClient := data_manager.NewDataManagerClient(dataManagerConnect)

	orderServiceApi := api.NewOrderServiceApi(dataManagerClient)

	lsn, err := net.Listen("tcp", fmt.Sprintf(":%s", appPort))

	if err != nil {
		log.Fatal(err)
	}

	var orderServiceServer = grpc.NewServer()

	reflection.Register(orderServiceServer)
	order_service.RegisterOrderServiceServer(orderServiceServer, orderServiceApi)

	if err := orderServiceServer.Serve(lsn); err != nil {
		log.Fatalf("serving error %v", err)
	}
}
