.PHONY: up down test test-java test-python test-harness conformance smoke bench jmh microbench report

up:                  ## Kafka + both servers (python in 1- and 2-process modes)
	docker compose up -d --build --wait kafka java-server java2-server python-server python2-server
	docker compose --profile tools build harness

down:
	docker compose down

test: test-java test-python test-harness

test-java:
	cd java-server && mvn -B -q verify

test-python:
	cd python-server && uv run pytest -q --cov

test-harness:
	cd harness && uv run pytest -q tests/test_hist.py

conformance: up      ## same behaviour from every implementation, against real Kafka
	cd harness && uv run pytest -v tests/test_conformance.py

smoke: up            ## ~1 minute
	cd harness && uv run python -m bench.run --suite smoke --force

bench: up            ## every experiment in docs/RESULTS.md (~2.5 hours)
	cd harness && uv run python -m bench.run --suite scaling --reps 3
	cd harness && uv run python -m bench.run --suite saturation
	cd harness && uv run python -m bench.run --suite slow --reps 2
	cd harness && uv run python -m bench.run --suite warmup

jmh:                 ## Java hot-path micro-benchmarks (JMH, with warm-up and forks)
	cd java-server && mvn -B -q -DskipTests package && java -jar jmh/target/benchmarks.jar -rf json -rff ../results/jmh.json

microbench:          ## the same operations in the Python server
	cd python-server && uv run python benchmarks/microbench.py | tee ../results/python-microbench.json

report:              ## charts + tables from results/
	cd harness && uv run --group report python -m bench.report
