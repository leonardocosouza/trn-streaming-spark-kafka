.PHONY: demo status parar reset

demo:    ## sobe todo o ambiente e espera o painel ficar pronto
	mkdir -p .demo/spark-events .demo/spark .demo/controle .demo/kafka
	docker compose up -d --build --wait
	@echo "Painel: http://localhost:8501"

status:  ## mostra os containers
	docker compose ps -a

parar:   ## para os containers, mantendo os dados
	docker compose stop

reset:   ## apaga tudo e volta ao estado inicial da demo
	docker compose down
	rm -rf .demo
	$(MAKE) demo
