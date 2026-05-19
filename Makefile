.PHONY: up down restart logs token ps clean test

up:
	docker compose up -d
	@echo "Waiting for Jupyter to initialize..."
	@sleep 10
	@$(MAKE) token

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f

token:
	@echo ""
	@echo "Jupyter URL:"
	@docker compose logs jupyter 2>&1 | grep -oE 'http://127.0.0.1:8888/lab\?token=[a-f0-9]+' | tail -1
	@echo ""
	@echo "Spark Master UI: http://localhost:8080"
	@echo "Spark App UI:    http://localhost:4040 (only while a job is running)"

ps:
	docker compose ps

test:
	docker compose exec jupyter python /home/jovyan/work/smoke_test.py

clean:
	docker compose down -v
	rm -rf notebooks/.ipynb_checkpoints
