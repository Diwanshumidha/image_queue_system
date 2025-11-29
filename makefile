GO := go

API_DIR := ./api
WORKER_DIR := ./worker

API_ENTRY := cmd/api/main.go
WORKER_ENTRY := .

# --- Run ---

api:
	@echo "Starting API..."
	@cd $(API_DIR) && $(GO) run $(API_ENTRY)

worker:
	@echo "Starting Worker..."
	@cd $(WORKER_DIR) && $(GO) run $(WORKER_ENTRY)

# Run both in parallel
dev:
	@echo "Starting API + Worker..."
	@cd $(API_DIR) && $(GO) run $(API_ENTRY) &
	@cd $(WORKER_DIR) && $(GO) run $(WORKER_ENTRY) &
	@wait

# --- Build ---

build-api:
	@echo "Building API..."
	@cd $(API_DIR) && $(GO) build -o ../bin/api $(API_ENTRY)

build-worker:
	@echo "Building Worker..."
	@cd $(WORKER_DIR) && $(GO) build -o ../bin/worker $(WORKER_ENTRY)

build: build-api build-worker

# --- Utilities ---

tidy:
	@echo "Tidying modules..."
	@cd $(API_DIR) && $(GO) mod tidy
	@cd $(WORKER_DIR) && $(GO) mod tidy
	@$(GO) work sync || true

clean:
	@echo "Cleaning build artifacts..."
	rm -rf ./bin

.PHONY: api worker dev build-api build-worker build tidy clean


DC = docker compose

up:
	$(DC) up -d

down:
	$(DC) down

restart:
	$(DC) down
	$(DC) up -d

logs:
	$(DC) logs -f

ps:
	$(DC) ps
