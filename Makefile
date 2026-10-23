CARGO ?= cargo
TOKEN ?= natslink-itest-token
NATS_IMAGE ?= nats:latest
C1 ?= natslink-nats-1
C2 ?= natslink-nats-2
P1 ?= 4222
P2 ?= 4223

.PHONY: fmt clippy test itest itest-up itest-down acl-test bench audit

fmt:
	$(CARGO) fmt --all -- --check

clippy:
	$(CARGO) clippy --locked --all-targets -- -D warnings

## Unit tests only (no server needed; integration tests return early without NATS_TEST_URL).
test: fmt clippy
	$(CARGO) test --locked

## Full suite against two throw-away docker NATS servers.
itest: itest-up
	@trap '$(MAKE) itest-down' EXIT; \
	NATS_TEST_URL='nats://$(TOKEN)@127.0.0.1:$(P1)' \
	NATS_TEST_URL2='nats://$(TOKEN)@127.0.0.1:$(P2)' \
	NATS_TEST_CONTAINER='$(C1)' \
	$(CARGO) test --locked -- --test-threads=4 2>&1 | tee itest.log; rc=$${PIPESTATUS[0]}; \
	exit $$rc

itest-up:
	docker run -d --name $(C1) -p 127.0.0.1:$(P1):4222 $(NATS_IMAGE) --auth $(TOKEN) >/dev/null
	docker run -d --name $(C2) -p 127.0.0.1:$(P2):4222 $(NATS_IMAGE) --auth $(TOKEN) >/dev/null
	@sleep 1

itest-down:
	-docker rm -f $(C1) $(C2) >/dev/null 2>&1

## Opt-in ACL hot-reload drill (spawns its own docker container).
acl-test:
	NATSLINK_ACL_DOCKER_TEST=1 $(CARGO) test --locked --test rejection -- --ignored --nocapture

bench:
	NATS_TEST_URL='nats://$(TOKEN)@127.0.0.1:$(P1)' NATS_TEST_URL2='nats://$(TOKEN)@127.0.0.1:$(P2)' \
	$(CARGO) run --release --example mirror_bench

audit:
	$(CARGO) audit
