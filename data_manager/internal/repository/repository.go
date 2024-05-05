package repository

import (
	"data_manager/internal/model"
	"fmt"
	"sync"
)

type OrderRepository interface {
	GetOrderByID(orderID int) (model.OrderData, error)
	InsertOrder(data model.OrderData) error
	GetAllOrders() []model.OrderData
}

type InMemoryOrderRepository struct {
	data  map[int]model.OrderData
	mutex sync.Mutex
}

func NewInMemoryOrderRepository() *InMemoryOrderRepository {
	return &InMemoryOrderRepository{
		data: make(map[int]model.OrderData),
	}
}

func (o *InMemoryOrderRepository) GetAllOrders() []model.OrderData {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	result := make([]model.OrderData, 0, len(o.data))

	for _, v := range o.data {
		result = append(result, v)
	}

	return result
}

func (o *InMemoryOrderRepository) GetOrderByID(orderID int) (model.OrderData, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	if order, ok := o.data[orderID]; ok {
		return order, nil
	}

	return model.OrderData{}, fmt.Errorf("order_service %d not found", orderID)
}

func (o *InMemoryOrderRepository) InsertOrder(data model.OrderData) error {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	orderID := data.OrderID

	if _, ok := o.data[orderID]; ok {
		return fmt.Errorf("order_service %d already exists", orderID)
	}

	o.data[orderID] = data

	return nil
}
