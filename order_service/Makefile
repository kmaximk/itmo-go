CURDIR=$(shell pwd)
PACKAGE=${CURDIR}/cmd/order_service
GENERATEDDIR=${CURDIR}/pkg/generated
BINDIR=${CURDIR}/bin
PROTOC=PATH="$$PATH:${BINDIR}/bin" protoc
LINTER=PATH="$$PATH:${BINDIR}" golangci-lint

build: bindir get-protoc install-lint generate-proto
	go mod tidy
	${LINTER} run
	go build -o ${BINDIR}/app ${PACKAGE}

generate-proto:
	rm -rf ${GENERATEDDIR}/proto
	mkdir -p ${GENERATEDDIR}/proto
	cd ${BINDIR}/protoc && \
	${PROTOC} \
	-I=${CURDIR}/internal/api/proto \
	-I=${CURDIR}/internal/clients/proto \
	--plugin=protoc-gen-go=$(BINDIR)/protoc-gen-go --go_out=${GENERATEDDIR}/proto \
	--plugin=protoc-gen-go-grpc=$(BINDIR)/protoc-gen-go-grpc --go-grpc_out=${GENERATEDDIR}/proto \
	${CURDIR}/internal/api/proto/order_service.proto \
	${CURDIR}/internal/clients/proto/data_manager.proto

bindir:
	rm -rf ${BINDIR}
	mkdir -p ${BINDIR}

get-protoc:
	rm -rf ${BINDIR}/protoc
	mkdir -p ${BINDIR}/protoc
	cd ${BINDIR} && \
	wget -O protoc.zip https://github.com/protocolbuffers/protobuf/releases/latest/download/protoc-25.1-linux-x86_64.zip && \
	unzip protoc.zip && \
	chmod a+x ${BINDIR}/bin/protoc
	(GOBIN=${BINDIR} go install google.golang.org/protobuf/cmd/protoc-gen-go@latest)
	(GOBIN=${BINDIR} go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest)

install-lint:
	GOBIN=${BINDIR} go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
