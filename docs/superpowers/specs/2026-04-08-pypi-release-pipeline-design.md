# PyPI Release Pipeline — Design Spec

**Date:** 2026-04-08
**Issue:** PLT-1251 — Release starfix-python v0.1.0 to PyPI
**Repo:** nauticalab/starfix-python

---

## Goal

Publish `starfix` v0.1.0 to PyPI via a best-practice tag-triggered CI/CD pipeline, with:
- Dynamic versioning derived from git tags (no manual version bumps)
- Two-stage publish: TestPyPI first, then PyPI with manual approval
- Cryptographic build attestations (supply chain security)
- License compliance checks on every push and PR

---

## Build Backend & Versioning

**Backend:** Keep existing `hatchling`. Add `hatch-vcs` as a second build-time dep for VCS-based dynamic versioning. This is 2 build deps total vs 3 for the `setuptools` + `setuptools-scm` path, and keeps the project on the modern stack rather than adopting a legacy backend.

### `pyproject.toml` changes

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[project]
# remove: version = "0.0.2"
dynamic = ["version"]

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/starfix/_version.py"
```

`src/starfix/_version.py` is auto-generated at build time from the git tag. It is excluded from version control via `.gitignore`.

**Tag format:** `v0.1.0` → published version `0.1.0`. Tags must match `v[0-9]+.[0-9]+.[0-9]+`.

---

## CI Workflow — `ci.yml` (additions)

Two new jobs added to the existing test workflow, running alongside the test matrix:

### `license-check` (runtime, catches transitives)

```yaml
license-check:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: astral-sh/setup-uv@v5
    - run: uv sync --dev
    - run: uv run pip-licenses \
        --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0" \
        --fail
```

`pip-licenses` added to `[dependency-groups] dev` in `pyproject.toml`. Fails the build if any installed dependency (including transitives) carries a license outside the allowlist.

### `dependency-review` (PR manifest-level, zero extra deps)

```yaml
dependency-review:
  runs-on: ubuntu-latest
  if: github.event_name == 'pull_request'
  steps:
    - uses: actions/checkout@v4
    - uses: actions/dependency-review-action@v4
      with:
        deny-licenses: >-
          GPL-2.0-only, GPL-2.0-or-later,
          GPL-3.0-only, GPL-3.0-or-later,
          AGPL-3.0-only, AGPL-3.0-or-later,
          LGPL-2.0-only, LGPL-2.1-only, LGPL-3.0-only
```

Runs only on pull requests. Reads the dependency graph diff and blocks PRs that introduce copyleft-licensed packages before they are even installed.

---

## Publish Workflow — `publish.yml` (new)

Triggered exclusively on version tag pushes matching `v[0-9]+.[0-9]+.[0-9]+`.

### Job chain

```
test → build → publish-testpypi → publish-pypi
```

### `test`

Mirrors the existing CI test matrix (Python 3.10/3.11/3.12) as a hard gate.
**Must use `fetch-depth: 0`** on checkout so `hatch-vcs` can read the full tag history.

### `build`

- `actions/checkout@v4` with `fetch-depth: 0`
- `uv build` produces wheel + sdist
- Uploads `dist/` as a workflow artifact shared by both publish jobs

### `publish-testpypi`

- Uses GitHub environment `testpypi` (no protection rules — auto-deploys)
- Permission: `id-token: write` (OIDC Trusted Publishing, no secrets)
- `pypa/gh-action-pypi-publish@release/v1` with `repository-url: https://upload.test.pypi.org/legacy/`
- `attestations: true` (PEP 740 / Sigstore signing)

### `publish-pypi`

- `needs: publish-testpypi`
- Uses GitHub environment `pypi` (**required-reviewer protection rule** — human must approve in GitHub UI)
- Same OIDC + attestations pattern
- After publish: `softprops/action-gh-release@v2` with `generate_release_notes: true` creates a GitHub Release with PR-derived changelog

---

## GitHub Environment Setup (one-time manual)

| Environment | Settings |
|---|---|
| `testpypi` | No protection rules |
| `pypi` | Required reviewers: repo owner |

Both created under: **GitHub repo → Settings → Environments**

---

## PyPI Trusted Publisher Setup (one-time manual)

Configure on both indexes before pushing the first tag.

| Field | Value |
|---|---|
| Owner | `nauticalab` |
| Repository | `starfix-python` |
| Workflow filename | `publish.yml` |
| Environment (TestPyPI) | `testpypi` |
| Environment (PyPI) | `pypi` |

- **TestPyPI:** https://test.pypi.org → Account Settings → Publishing → Add a new pending publisher
- **PyPI:** https://pypi.org → Account Settings → Publishing → Add a new pending publisher

---

## Files Changed / Created

| File | Change |
|---|---|
| `pyproject.toml` | Switch to `hatchling`+`hatch-vcs`, dynamic version, add `pip-licenses` to dev deps |
| `.gitignore` | Add `src/starfix/_version.py` |
| `.github/workflows/ci.yml` | Add `license-check` and `dependency-review` jobs |
| `.github/workflows/publish.yml` | New — tag-triggered TestPyPI → PyPI pipeline |

---

## Out of Scope

- Changes to `src/` or `tests/` (no functional code changes)
- Updating `orcapod-python`'s dependency specifier to remove the git URL (tracked in PLT-1250)
- Setting up PyPI Trusted Publishers (manual step documented above, performed by repo owner)
- Configuring `license-check` and `dependency-review` as required branch-protection status checks (one-time GitHub Settings → Branches step, optional but recommended)
