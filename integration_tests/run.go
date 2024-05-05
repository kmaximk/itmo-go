package main

import (
	"context"
	"log"
	"os/exec"
	"os/signal"
	"syscall"
)

func main() {
	ctx := context.Background()
	ctx, _ = signal.NotifyContext(ctx, syscall.SIGTERM)

	orderServiceStart := exec.Command("../order_service/bin/app", "-data_manager_address", "localhost:8086", "-app_port", "8085")
	dataManagerServiceStart := exec.Command("../data_manager/bin/app", "-order_address", "localhost:8085", "-app_port", "8086")

	go func() {
		defer func() {
			err := orderServiceStart.Process.Kill()

			if err != nil {
				log.Println(err)
			}
		}()

		if err := orderServiceStart.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	go func() {
		defer func() {
			err := dataManagerServiceStart.Process.Kill()

			if err != nil {
				log.Println(err)
			}
		}()

		if err := dataManagerServiceStart.Run(); err != nil {
			log.Fatal(err)
		}
	}()

	<-ctx.Done()
}
