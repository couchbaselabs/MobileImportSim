# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOCLEAN=$(GOCMD) clean
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
BINARY_NAME=mobileImportSim
GOMOD_FILE=go.mod
GOMOD_SUM=go.sum

all: build
build: 
	$(GOBUILD) -o $(BINARY_NAME) -v
clean: 
	rm $(GOMOD_FILE)
	rm $(GOMOD_SUM)
	rm -f $(BINARY_NAME)
	$(GOCLEAN) -modcache
deps:
	$(GOMOD) init mobileImportSim
	$(GOGET) github.com/couchbase/gocbcore/v10
	$(GOGET) github.com/couchbase/gocb/v2
	$(GOGET) github.com/couchbaselabs/gojsonsm@v1.0.1
	$(GOGET) github.com/couchbase/cbauth@v0.1.5
	$(GOGET) github.com/couchbase/goxdcr@v8.0.0-1633
	$(GOGET) github.com/couchbase/gomemcached@v0.3.1