watch:
  cargo watch -i "**/*.snap.new" -x test -x clippy

format:
  cargo fmt --all

machete:
  cargo machete

fix:
  cargo clippy --fix --no-deps --allow-dirty

run sql:
  cargo run --bin cli -- --sql '{{ sql }}' | jq
