get-jupyter-token:
	docker compose -f docker-compose.yml exec jupyter-local jupyter notebook list

up-build:
	docker compose up --build
