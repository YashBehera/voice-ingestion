.PHONY: all build run test clean experiment help

IMAGE_NAME = voice-ingestion

all: build

build:
	@echo "Building Docker image $(IMAGE_NAME)..."
	docker build -t $(IMAGE_NAME):latest .

run:
	@echo "Running Voice Ingestion Worker container..."
	@echo "Access the dashboard at http://localhost:8080"
	@echo "RTP port listening at udp://localhost:5004"
	docker run --rm -it \
		-p 8080:8080 \
		-p 5004:5004/udp \
		-v $$(pwd)/recordings:/app/recordings \
		$(IMAGE_NAME):latest

test:
	@echo "Running Go tests inside Docker container..."
	# We build the builder stage to run go test
	docker build --target builder -t $(IMAGE_NAME)-builder:latest .
	docker run --rm -it $(IMAGE_NAME)-builder:latest go test -v -race ./...

clean:
	@echo "Cleaning up local files and recordings..."
	rm -rf recordings/*.wav
	docker rmi $(IMAGE_NAME):latest $(IMAGE_NAME)-builder:latest 2>/dev/null || true

experiment:
	@echo "Instructions for Packet Loss Experiment:"
	@echo "1. Run the worker: 'make run'"
	@echo "2. In a separate terminal, run a sender with 20% packet loss (No RED):"
	@echo "   docker run --rm --net=host -v \$$(pwd)/testdata:/testdata $(IMAGE_NAME):latest /app/sender --host 127.0.0.1 --port 5004 --file /testdata/input.wav --loss 20 --red=false"
	@echo "3. Run the sender with 20% packet loss (With RED):"
	@echo "   docker run --rm --net=host -v \$$(pwd)/testdata:/testdata $(IMAGE_NAME):latest /app/sender --host 127.0.0.1 --port 5004 --file /testdata/input.wav --loss 20 --red=true"

help:
	@echo "Voice Ingestion Worker Makefile targets:"
	@echo "  make build      - Build multi-stage Docker image"
	@echo "  make run        - Run worker container (binds to 8080 and 5004/udp)"
	@echo "  make test       - Run unit/integration tests in Docker builder environment"
	@echo "  make clean      - Clean up artifacts and local Docker build images"
	@echo "  make experiment - Show commands to run the packet loss experiments"
