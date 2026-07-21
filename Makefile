.PHONY: check

check:
	cargo fmt --all --check
	cargo clippy --locked --all-targets --all-features -- -D warnings
	cargo test --locked --all-targets --all-features
