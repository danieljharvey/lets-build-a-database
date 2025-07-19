watch:
  cargo watch -i 'test_storage*' -x test -x clippy

format:
  cargo fmt --all

machete:
  cargo machete

fix:
  cargo clippy --fix --no-deps --allow-dirty

