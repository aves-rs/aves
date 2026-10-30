.PHONY: zedy lint test build

clean:
	direnv exec . cargo clean --profile=dev

fmt:
	direnv exec . cargo fmt

check:
	direnv exec . cargo check

build:
	direnv exec . cargo build --profile=dev

lint:
	direnv exec . cargo clippy -- --deny warnings

test:
	direnv exec . cargo test

test_ci:
	direnv exec . cargo test --no-default-features

zedy:
	$(MAKE) fmt
	$(MAKE) check
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) build
