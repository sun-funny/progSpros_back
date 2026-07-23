SHELL := /bin/bash

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
	docker exec -t progSpros_PGDB pg_dumpall -c -U postgres > progSpros_PGDB_`date +%Y-%m-%d"_"%H_%M_%S`.sql
down-dev:
	docker compose -f docker-compose.dev.yaml down
psql-db:
	docker exec -it progSpros_PGDB psql -U postgres -d progSpros
update-templates:
	docker cp ./progSpros_back/templates/. progSpros_backend:/progSpros_back/templates/
update-reqs:
	docker cp ./expreport_backend/requirements.txt expr_backend:/opt/foresight/expreport_backend/
	docker exec -t expr_backend pip3 install --no-cache-dir -r /opt/foresight/expreport_backend/requirements.txt
update-db:
	docker exec -i progSpros_PGDB bash -c "psql -U postgres -c \"SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = 'progSpros' AND pid <> pg_backend_pid();\" -c \"DROP DATABASE IF EXISTS progSpros;\" -c \"CREATE DATABASE progSpros;\""
	docker exec -i progSpros_PGDB psql -U postgres -d progSpros < Progn_Spros.sql

deploy-backend:
	$(eval DATE := $(shell date +%d_%m_%Y))
	$(eval DIRNAME := progSpros_DEPLOY_$(DATE))
	$(eval TMPDIR := /tmp/$(DIRNAME))
	mkdir -p "$(TMPDIR)"
	cp -r progSpros_back/. "$(TMPDIR)/"
	find "$(TMPDIR)" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find "$(TMPDIR)" -name "*.pyc" -delete
	rm -f "$(TMPDIR)/config_ps.py" "$(TMPDIR)/Progn_Spros_app.py"
	find "$(TMPDIR)" -maxdepth 1 -iname "dockerfile*" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname ".flaskenv" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname "requirements*.txt" -delete
	cd /tmp && zip -r "$(DIRNAME).zip" "$(DIRNAME)"
	mv "/tmp/$(DIRNAME).zip" "/tmp/$(DIRNAME).pefx"
	rm -rf "$(TMPDIR)"
	mv "/tmp/$(DIRNAME).pefx" .
	@echo "Created $(DIRNAME).pefx"

deploy-backend-staged:
	$(eval DATE := $(shell date +%d_%m_%Y))
	$(eval DIRNAME := progSpros_UPDATE_$(DATE))
	$(eval TMPDIR := /tmp/$(DIRNAME))
	rm -rf "$(TMPDIR)" && mkdir -p "$(TMPDIR)"
	git diff --cached --name-only | grep '^progSpros_back/' | sed 's|^progSpros_back/||' | \
		rsync -a --files-from=- progSpros_back/ "$(TMPDIR)/"
	find "$(TMPDIR)" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find "$(TMPDIR)" -name "*.pyc" -delete
	rm -f "$(TMPDIR)/config_ps.py" "$(TMPDIR)/Progn_Spros_app.py"
	find "$(TMPDIR)" -maxdepth 1 -iname "dockerfile*" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname ".flaskenv" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname "requirements*.txt" -delete
	cd /tmp && zip -r "$(DIRNAME).zip" "$(DIRNAME)"
	mv "/tmp/$(DIRNAME).zip" "/tmp/$(DIRNAME).pefx"
	rm -rf "$(TMPDIR)"
	mv "/tmp/$(DIRNAME).pefx" .
	@echo "Created $(DIRNAME).pefx"


deploy-backend-since:
	@test -n "$(COMMIT)" || { echo "Usage: make deploy-backend-since COMMIT=<hash> [FILENAME=<name>] NOTE: !!! включает незакомиченные изменения тоже"; exit 1; }
	$(eval DATE := $(shell date +%d_%m_%Y))
	$(eval DIRNAME := $(if $(FILENAME),$(FILENAME),progSpros_upd_$(DATE)))
	$(eval TMPDIR := /tmp/$(DIRNAME))
	rm -rf "$(TMPDIR)" && mkdir -p "$(TMPDIR)"
	git diff -z --name-only --diff-filter=d "$(COMMIT)^" HEAD | grep -z '^progSpros_back/' | while IFS= read -r -d '' file; do \
		rel="$${file#progSpros_back/}"; \
		mkdir -p "$(TMPDIR)/$$(dirname "$$rel")"; \
		cp -- "$$file" "$(TMPDIR)/$$rel"; \
	done
	find "$(TMPDIR)" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find "$(TMPDIR)" -name "*.pyc" -delete
	rm -f "$(TMPDIR)/config_ps.py" "$(TMPDIR)/Progn_Spros_app.py"
	find "$(TMPDIR)" -maxdepth 1 -iname "dockerfile*" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname ".flaskenv" -delete
	find "$(TMPDIR)" -maxdepth 1 -iname "requirements*.txt" -delete
	cd /tmp && zip -r "$(DIRNAME).zip" "$(DIRNAME)"
	mv "/tmp/$(DIRNAME).zip" "/tmp/$(DIRNAME).pefx"
	rm -rf "$(TMPDIR)"
	mv "/tmp/$(DIRNAME).pefx" .
	@echo "Created $(DIRNAME).pefx"
