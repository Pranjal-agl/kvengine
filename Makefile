.PHONY: build test test-race test-short bench fuzz flamegraph fmt vet clean

build:
	go build ./...

test:
	go test ./...

test-race:
	go test -race ./...

test-short:
	go test -short ./...

bench:
	go test -bench=. -benchtime=3s ./internal/store/...
	go test -bench=. -benchtime=3s ./internal/server/...
	go test -bench=. -benchtime=3s ./internal/lsm/...

# Run all fuzz targets for 30s each (change FUZZ_SECS to adjust)
FUZZ_SECS ?= 30
fuzz:
	chmod +x scripts/fuzz.sh
	./scripts/fuzz.sh $(FUZZ_SECS)

# Run fuzz seed corpus only (fast — no fuzzing, just crash-checks seeds)
fuzz-seeds:
	go test -run 'Fuzz' ./internal/wal/... ./internal/server/... ./internal/raft/...

# Generate CPU flamegraph (requires server to be buildable; opens profiles/cpu.svg)
flamegraph:
	chmod +x scripts/flamegraph.sh
	./scripts/flamegraph.sh

fmt:
	gofmt -l -w .

vet:
	go vet ./...

clean:
	rm -f kvengine
	rm -rf profiles/
