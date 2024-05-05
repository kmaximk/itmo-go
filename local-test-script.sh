(cd order_service && make build)
(cd data_manager && make build)
chmod +x ./order_service/bin/app
chmod +x ./data_manager/bin/app
go test ./integration_tests/integration_test.go
