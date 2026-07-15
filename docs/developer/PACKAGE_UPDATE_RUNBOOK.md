# Package Update Runbook

How to update Python dependencies in this project.

## Bump a single package

Use when a new version is available and `pyproject.toml` doesn't need changing (i.e., the existing version specifier already allows the new version).

```bash
# 1. Single command — bumps only the target, keeps everything else pinned
pip-compile --generate-hashes --output-file=requirements.lock pyproject.toml \
    --upgrade-package <package-name>

# 2. Verify only the target package changed version
diff <(git show HEAD:requirements.lock | rg -o '^[a-z][a-z0-9_.-]*==[0-9][0-9.]*' | sort) \
     <(rg -o '^[a-z][a-z0-9_.-]*==[0-9][0-9.]*' requirements.lock | sort)

# 3. Verify lock file is complete (~2300 lines, not ~150)
wc -l requirements.lock

# 4. Audit + test
make audit
pytest -q

# 5. Commit
git add requirements.lock
git commit -m "chore: bump <package-name> X.Y.Z → A.B.C"
```

### Pitfall: truncated lock file

Running `make lock` (full regeneration) followed by a separate `pip-compile --upgrade-package` can truncate the lock file to ~150 lines containing only the target package's dependency chain. This happens because the second invocation doesn't use the first run's output as a constraint baseline.

**Always use a single `pip-compile --upgrade-package` invocation.** Do not run `make lock` first.

If you accidentally truncate the file:
```bash
git checkout -- requirements.lock   # restore the full lock
# then re-run the single upgrade command above
```

## Bump a package that needs a pyproject.toml change

Use when you need to change the version specifier (e.g., raise a floor pin).

```bash
# 1. Edit pyproject.toml
# 2. Full regeneration
make lock
make audit
pytest -q
# 3. Commit both files
git add pyproject.toml requirements.lock
```

## Upgrade all packages

Rarely needed. Regenerates everything from scratch.

```bash
pip-compile --generate-hashes --output-file=requirements.lock pyproject.toml --upgrade
make audit
pytest -q
```

## After any update

- Check `make audit` output — it should pass cleanly except for explicitly documented waiver flags kept in sync across `Makefile`, CI, and `docs/audits/SUPPLY_CHAIN_PROTECTION.md`
- Run tests to catch breaking changes
- If tests reference changed APIs (e.g., mock stubs for removed methods), update them
- Deploy is separate — don't deploy in the same session unless intended

### Gotcha: `make audit` does not touch the runtime env

`make audit` scans `requirements.lock`. It does NOT verify that the installed Python environment matches the lock file. If you bump a pin, regenerate the lock, and run `make audit` → clean, the packages in the *lock file* have no known CVEs — but the packages actually loaded by running services are whatever was installed last time `pip install` ran.

**Symptom (2026-04-15 smoke test):** six deps bumped in the lockfile, `make audit` clean, `pytest` green, both `finance_gateway` and `finance_web_backend` services restarted — yet `python3 -c "import anthropic; print(anthropic.__version__)"` still reported the old version. The services restarted with stale code because Python's runtime environment had never been reinstalled.

**Always run this after updating `requirements.lock`:**
```bash
pip install --require-hashes -r requirements.lock
# Then restart any long-running services that import the updated packages.
```

Verify the runtime picked up the new versions before declaring the update done:
```bash
python3 -c "import anthropic, fastmcp, aiohttp, cryptography, PIL, pygments; \
print(f'anthropic={anthropic.__version__}, fastmcp={fastmcp.__version__}, \
aiohttp={aiohttp.__version__}, cryptography={cryptography.__version__}, \
PIL={PIL.__version__}, pygments={pygments.__version__}')"
```
Note `pillow` imports as `PIL`; `pdfminer.six` imports as `pdfminer`.

In production this is less of a foot-gun because `scripts/deploy_web.sh` runs `pip install` as part of every deploy. Locally, it's easy to miss.
