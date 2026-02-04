blob-simulator:
	@if docker ps -q -f name=azurite | grep -q .; then \
		echo "Azurite is already running"; \
	elif docker ps -aq -f name=azurite | grep -q .; then \
		echo "Starting existing Azurite container..."; \
		docker start azurite; \
	else \
		echo "Creating new Azurite container..."; \
		docker run -d --name azurite -p 10000:10000 -p 10001:10001 -p 10002:10002 mcr.microsoft.com/azure-storage/azurite; \
	fi

test: blob-simulator
	mix test --include integration
