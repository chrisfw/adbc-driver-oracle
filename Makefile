BINARY_NAME=libadbc_driver_oracle
GOFLAGS=-buildmode=c-shared -tags driverlib

.PHONY: all build test clean

all: build

build:
	go build $(GOFLAGS) -o $(BINARY_NAME).dylib ./pkg/oracle/

build-linux:
	GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -o $(BINARY_NAME).so ./pkg/oracle/

# Windows DLL, cross-compiled with mingw-w64 (apt install gcc-mingw-w64-x86-64).
build-windows:
	GOOS=windows GOARCH=amd64 CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc \
		go build $(GOFLAGS) -trimpath -ldflags '-s -w' -o $(BINARY_NAME).dll ./pkg/oracle/

# Build CLI test harness (standalone binary, no CGO exports)
build-cli:
	go build -o oracle-adbc-test ./cmd/

test:
	go test -v ./go/...

clean:
	rm -f $(BINARY_NAME).dylib $(BINARY_NAME).so $(BINARY_NAME).dll $(BINARY_NAME).h oracle-adbc-test
