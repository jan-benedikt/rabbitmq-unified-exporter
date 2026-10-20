VERSION ?= 1.0.0
IMAGE ?= rabbitmq-unified-exporter:$(VERSION)

.PHONY: build test race lint docker helm alertlint

build:
	go build -ldflags="-X main.version=$(VERSION)" -o bin/rabbitmq-unified-exporter ./cmd/rabbitmq-unified-exporter

test:
	go test -count=1 ./...

race:
	go test -race -count=1 -timeout 120s ./...

lint:
	go vet ./...

docker:
	docker build --build-arg VERSION=$(VERSION) -t $(IMAGE) .

helm:
	helm lint deploy/helm/rabbitmq-unified-exporter
	helm template test deploy/helm/rabbitmq-unified-exporter >/dev/null

alertlint:
	go run ./tools/alertlint -alerts alerts.yaml,alerts/rabbitmq-availability-alerts.migrated.yml,alerts/rabbitmq_alerts.migrated.yaml

integration:
	go test -tags=integration -count=1 -timeout 10m ./internal/integration/...
