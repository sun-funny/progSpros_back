build-dev:
	docker-compose -f docker-compose.dev.yaml up -d db 
	docker exec -i progSpros_PGDB psql psql -v ON_ERROR_STOP=1 -p 5432 -U postgres -d progSpros < Progn_Spros.sql
	docker-compose -f docker-compose.dev.yaml up backend
run-dev:
	docker compose -f 'docker-compose.dev.yaml' up -d --build
	cd frontend && npm run dev
	nohup npm run dev &
	echo $! > client_pid.txt
	cd ..
	docker-compose -f docker-compose.dev.yaml logs --follow
dump-db:
	docker exec -t expr_PGDB pg_dumpall -c -U postgres > EEP_`date +%Y-%m-%d"_"%H_%M_%S`.sql
down-dev:
	docker compose -f docker-compose.dev.yaml down
psql-db:
	docker exec -it progSpros_PGDB psql -U postgres -d progSpros
update-templates:
	docker cp ./templates/. progSpros_backend:/progSpros_back/templates/
update-reqs:
	docker cp ./expreport_backend/requirements.txt expr_backend:/opt/foresight/expreport_backend/
	docker exec -t expr_backend pip3 install --no-cache-dir -r /opt/foresight/expreport_backend/requirements.txt
update-db:
	docker exec -i b4d2c6cd0af7_expr_PGDB bash -c "psql -U postgres -c \"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'expreport' AND pid <> pg_backend_pid();\" -c \"DROP DATABASE IF EXISTS expreport;\" -c \"CREATE DATABASE expreport;\""
	docker exec -i b4d2c6cd0af7_expr_PGDB pg_restore -U postgres -d expreport < expreport_backend/db.dmp