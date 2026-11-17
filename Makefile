.PHONY: run test build algo backtest
run:
	go run ./cmd/server
build:
	go build -o bin/simulator ./cmd/server
test:
	go test -race ./...
	go vet ./...
	PYTHONPATH=python python3 -m unittest discover -s python/tests -v
algo:
	PYTHONPATH=python python3 -m strategy.runner
backtest:
	PYTHONPATH=python python3 -m strategy.sample --output sample.csv
	PYTHONPATH=python python3 -m strategy.backtest sample.csv --output backtest.json
