# Changelog

## 1.5.2-emqx.1 (unreleased)

- Add `overflow_ttl` and `overflow_check_period`, both in milliseconds, to
  retain idle excess workers between bursts and reap them after their idle TTL.
  TTL defaults to zero, preserving immediate reaping. With TTL enabled, the
  default check interval is the smaller of the TTL and 1000 milliseconds.
- Reap only idle workers and keep at least the configured base pool size.
  Preserve FIFO/LIFO ordering and hand waiting callers an available worker.
- Report retained idle workers as available at the overflow limit and account
  for excess idle workers that exit.
- Include upstream master changes since `1.5.2`: queue-based idle-worker storage
  for consistent FIFO/LIFO performance and map-format supervisor child specs.
- Migrate legacy `1.5.2` list-based and master queue-based pool states through
  `code_change/3`, with TTL disabled for existing pools. Restart pools to apply
  the new options. Hot upgrades must invoke state migration or restart pools;
  loading the new module alone does not migrate running state.

This version extends the EMQX fork. The existing `1.5.2` tag remains unchanged.
