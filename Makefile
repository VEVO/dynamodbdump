default: dep lint test

dep:
	go get -v ./...

fmt:
	@gofmt -s -w .

lint: fmt
	go get github.com/golang/lint/golint
	golint -set_exit_status ./...
	go vet -v ./...

gocov:
	go get github.com/axw/gocov/gocov
	go install github.com/axw/gocov/gocov
	@gocov test | gocov report
	# gocov test >/tmp/gocovtest.json ; gocov annotate /tmp/gocovtest.json MyFunc

test:
	go test -v ./...

build: dep lint test
	go clean -v
	go build -v

install: dep
	go install
