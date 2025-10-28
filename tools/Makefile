.PHONY: build-atas run-binance schema-check

build-atas:
	DOTNET_CLI_TELEMETRY_OPTOUT=1 dotnet build -c Release tools/atas/AtasIndicators.csproj

run-binance:
	@if [ -z "$(SYMBOL)" ] || [ -z "$(DATE)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make run-binance SYMBOL=BTCUSDT DATE=2024-01-15 OUT=./data.csv"; \
		exit 1; \
	fi
	python tools/binance/fetch_binance_1m.py --symbol $(SYMBOL) --date $(DATE) --out $(OUT)

schema-check:
	@echo "schema-check placeholder (QC pipeline integration pending)"
