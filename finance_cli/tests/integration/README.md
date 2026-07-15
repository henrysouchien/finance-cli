# Local MCP Integration Tests

These tests require:

- `TEST_POSTGRES_URL` pointing at a real PostgreSQL instance.
- `sqlcipher3` installed in the active Python environment.
- `CI_REQUIRE_INTEGRATION=1` to fail closed when `TEST_POSTGRES_URL` is missing or unreachable.

Local run:

```bash
TEST_POSTGRES_URL=postgresql://... pytest -m integration finance_cli/tests/integration/
```

Deploy-gate run:

```bash
CI_REQUIRE_INTEGRATION=1 TEST_POSTGRES_URL=postgresql://... pytest -m integration finance_cli/tests/integration/
```
