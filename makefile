lint:
	cargo fmt
	cargo clippy --all-targets --all-features -- -D warnings

fix:
	cargo fix --allow-dirty
