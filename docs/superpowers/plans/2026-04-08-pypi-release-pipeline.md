# PyPI Release Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire up tag-triggered PyPI publishing for `nauticalab/starfix-python` with dynamic VCS versioning, two-stage publish (TestPyPI → PyPI), and license compliance checks.

**Architecture:** Switch the build backend from plain `hatchling` to `hatchling` + `hatch-vcs` so package version is derived from git tags automatically. Add a `publish.yml` workflow that runs on `v*.*.*` tag pushes and chains: test → build → publish-testpypi (auto) → publish-pypi (manual approval gate via GitHub environment). Extend `ci.yml` with two license-check jobs.

**Tech Stack:** hatchling, hatch-vcs, uv, GitHub Actions, PyPI Trusted Publishing (OIDC), pypa/gh-action-pypi-publish, pip-licenses, actions/dependency-review-action

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Add `hatch-vcs` build dep, switch to dynamic version, configure VCS source, add `pip-licenses` to dev deps |
| `.gitignore` | Modify | Exclude auto-generated `src/starfix/_version.py` |
| `.github/workflows/ci.yml` | Modify | Add `license-check` and `dependency-review` jobs |
| `.github/workflows/publish.yml` | Create | Full tag-triggered TestPyPI → PyPI publish pipeline |

---

## Pre-flight: Authenticate and branch

- [ ] **Authenticate with GitHub org**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
```

- [ ] **Create a working branch**

```bash
cd /home/kurouto/Projects/starfix-python
git checkout -b feat/plt-1251-pypi-release-pipeline
```

- [ ] **Set git identity**

```bash
git config user.name "agent-kurouto[bot]"
git config user.email "268466204+agent-kurouto[bot]@users.noreply.github.com"
```

---

## Task 1: Switch to dynamic VCS versioning

**Files:**
- Modify: `pyproject.toml`

### Background

`hatch-vcs` is a hatchling build hook that calls into `setuptools-scm` to derive the package version from git tags at build time. It writes a `_version.py` file into the source tree. With no tag present it produces a dev version (e.g. `0.1.0.dev3+gabcdef`); with tag `v0.1.0` it produces `0.1.0`.

- [ ] **Replace the `[build-system]` block**

In `pyproject.toml`, change:

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"
```

To:

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"
```

- [ ] **Switch `version` from static to dynamic**

Remove the static field and add the dynamic declaration:

```toml
[project]
name = "starfix"
dynamic = ["version"]          # ← replaces: version = "0.0.2"
```

- [ ] **Add VCS version source and hook config**

Append these two sections anywhere after `[project]`:

```toml
[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/starfix/_version.py"
```

- [ ] **Add `pip-licenses` to dev deps**

```toml
[dependency-groups]
dev = ["pytest>=9.0.2", "pip-licenses>=5.0.0"]
```

- [ ] **Verify the full `pyproject.toml` looks like this**

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[project]
name = "starfix"
dynamic = ["version"]
description = "Pure-Python implementation of starfix Arrow logical hasher"
readme = "README.md"
requires-python = ">=3.10"
license = "MIT OR Apache-2.0"
authors = [{ name = "nauticalab" }]
keywords = ["arrow", "hashing"]
classifiers = [
    "Programming Language :: Python :: 3",
]
dependencies = ["pyarrow>=14.0.0"]

[dependency-groups]
dev = ["pytest>=9.0.2", "pip-licenses>=5.0.0"]

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/starfix/_version.py"

[tool.hatch.build.targets.wheel]
packages = ["src/starfix"]
```

- [ ] **Sync dependencies to verify the config parses correctly**

```bash
uv sync --dev
```

Expected: resolves and installs without error. `hatch-vcs` and `pip-licenses` appear in the output.

- [ ] **Build the package to verify VCS versioning works**

```bash
uv build
```

Expected: no errors. Two files created in `dist/`:
- `starfix-X.Y.Z.devN+gHASH-py3-none-any.whl` (dev version — no tag yet, that's fine)
- `starfix-X.Y.Z.devN+gHASH.tar.gz`

If the build fails with "setuptools-scm could not detect version", ensure you are inside the git repo and `git log` shows commits.

- [ ] **Verify pip-licenses passes against the current dependency set**

```bash
uv run pip-licenses --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0"
```

Expected: exits 0, lists `pyarrow` as Apache-2.0. If any package fails the allowlist, investigate before proceeding.

- [ ] **Commit**

```bash
git add pyproject.toml
git commit -m "build: switch to hatchling+hatch-vcs for tag-based dynamic versioning (PLT-1251)"
```

---

## Task 2: Exclude auto-generated `_version.py` from git

**Files:**
- Modify: `.gitignore`

`hatch-vcs` writes `src/starfix/_version.py` at build time. It must never be committed — its content is always derived from the current git state.

- [ ] **Append to `.gitignore`**

```
# hatch-vcs generated version file
src/starfix/_version.py
```

- [ ] **Verify the file is not already tracked**

```bash
git status src/starfix/_version.py
```

Expected: either "nothing to commit" (file doesn't exist yet) or shown as untracked/ignored. If it shows as tracked, run `git rm --cached src/starfix/_version.py` before committing.

- [ ] **Commit**

```bash
git add .gitignore
git commit -m "chore: exclude hatch-vcs generated _version.py from git (PLT-1251)"
```

---

## Task 3: Add license compliance jobs to CI

**Files:**
- Modify: `.github/workflows/ci.yml`

Two jobs are added: `license-check` (runs on every push, catches transitive deps) and `dependency-review` (runs on PRs only, reads the GitHub dependency graph diff).

- [ ] **Add `license-check` job**

Append to `.github/workflows/ci.yml` after the existing `test` job:

```yaml
  license-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Install dependencies
        run: uv sync --dev

      - name: Check dependency licenses
        run: >-
          uv run pip-licenses
          --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0"
          --fail

  dependency-review:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4

      - name: Dependency review
        uses: actions/dependency-review-action@v4
        with:
          deny-licenses: >-
            GPL-2.0-only, GPL-2.0-or-later,
            GPL-3.0-only, GPL-3.0-or-later,
            AGPL-3.0-only, AGPL-3.0-or-later,
            LGPL-2.0-only, LGPL-2.1-only, LGPL-3.0-only
```

- [ ] **Verify the full `ci.yml` is valid YAML**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml')); print('OK')"
```

Expected: `OK`

- [ ] **Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add license-check and dependency-review jobs (PLT-1251)"
```

---

## Task 4: Create the publish workflow

**Files:**
- Create: `.github/workflows/publish.yml`

This is the core of the pipeline. The `publish-pypi` job targets the `pypi` GitHub environment, which requires a human to approve before deploying. Both publish jobs use OIDC Trusted Publishing (no API tokens in secrets) and generate Sigstore attestations.

- [ ] **Create `.github/workflows/publish.yml` with this exact content**

```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v[0-9]+.[0-9]+.[0-9]+"

jobs:
  test:
    name: Test (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      matrix:
        python-version: ["3.10", "3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs needs full tag history

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Set up Python ${{ matrix.python-version }}
        run: uv python install ${{ matrix.python-version }}

      - name: Install dependencies
        run: uv sync --dev --python ${{ matrix.python-version }}

      - name: Run tests
        run: uv run --python ${{ matrix.python-version }} pytest tests/ -v

  build:
    name: Build distribution
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs needs full tag history

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Build wheel and sdist
        run: uv build

      - name: Upload dist artifact
        uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/
          if-no-files-found: error

  publish-testpypi:
    name: Publish → TestPyPI
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: testpypi
      url: https://test.pypi.org/p/starfix
    permissions:
      id-token: write   # required for OIDC Trusted Publishing
    steps:
      - name: Download dist artifact
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish to TestPyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          repository-url: https://upload.test.pypi.org/legacy/
          attestations: true

  publish-pypi:
    name: Publish → PyPI
    needs: publish-testpypi
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/starfix
    permissions:
      id-token: write   # required for OIDC Trusted Publishing
      contents: write   # required for creating GitHub Release
    steps:
      - name: Download dist artifact
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish to PyPI
        uses: pypa/gh-action-pypi-publish@release/v1
        with:
          attestations: true

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          generate_release_notes: true
          files: dist/*
```

- [ ] **Verify the workflow YAML is valid**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml')); print('OK')"
```

Expected: `OK`

- [ ] **Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: add tag-triggered TestPyPI → PyPI publish workflow (PLT-1251)"
```

---

## Task 5: Push branch and open PR

- [ ] **Push the branch**

```bash
git push -u origin feat/plt-1251-pypi-release-pipeline
```

- [ ] **Open a PR against `main`**

```bash
gh pr create \
  --title "ci: tag-triggered PyPI release pipeline with VCS versioning" \
  --body "$(cat <<'EOF'
## Summary

- Switches build backend to `hatchling` + `hatch-vcs` for tag-based dynamic versioning (`v0.1.0` → `0.1.0`)
- Adds `publish.yml`: tag-triggered workflow with test → build → TestPyPI (auto) → PyPI (manual approval gate)
- Uses OIDC Trusted Publishing (no secrets) and Sigstore build attestations
- Adds `license-check` and `dependency-review` jobs to `ci.yml` to block copyleft dependencies
- Adds `pip-licenses` to dev deps

## Test plan

- [ ] CI passes on this PR (test matrix + license-check)
- [ ] After merge: configure GitHub environments `testpypi` and `pypi` (see spec)
- [ ] After merge: configure Trusted Publishers on test.pypi.org and pypi.org (see spec)
- [ ] Push tag `v0.1.0` — verify TestPyPI publishes automatically
- [ ] Approve PyPI deployment in GitHub UI — verify pypi.org package and GitHub Release appear

Closes PLT-1251
EOF
)"
```

---

## Task 6: Tag v0.1.0 (post-merge, after manual setup)

> ⚠️ **Do not push this tag until:**
> 1. The PR is merged to `main`
> 2. GitHub environments `testpypi` and `pypi` are created (repo Settings → Environments)
> 3. Trusted Publishers are configured on both test.pypi.org and pypi.org

**Trusted Publisher values for both indexes:**

| Field | Value |
|---|---|
| PyPI project name | `starfix` |
| Owner | `nauticalab` |
| Repository | `starfix-python` |
| Workflow filename | `publish.yml` |
| Environment (TestPyPI publisher) | `testpypi` |
| Environment (PyPI publisher) | `pypi` |

Configure at:
- TestPyPI: https://test.pypi.org/manage/account/publishing/ → "Add a new pending publisher"
- PyPI: https://pypi.org/manage/account/publishing/ → "Add a new pending publisher"

- [ ] **After the PR is merged and setup is done, pull main and tag**

```bash
git checkout main
git pull origin main
git tag v0.1.0
git push origin v0.1.0
```

- [ ] **Watch the workflow run**

```bash
gh run watch
```

Expected sequence:
1. `test` matrix passes
2. `build` completes, produces `starfix-0.1.0-py3-none-any.whl` and `starfix-0.1.0.tar.gz`
3. `publish-testpypi` deploys to https://test.pypi.org/p/starfix
4. `publish-pypi` pauses with "Waiting for review"

- [ ] **Verify TestPyPI install works**

```bash
pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple/ starfix==0.1.0
python -c "import starfix; print('ok')"
```

- [ ] **Approve the PyPI deployment in GitHub UI**

Go to the workflow run → click "Review deployments" → Approve.

- [ ] **Verify PyPI install works**

```bash
pip install starfix==0.1.0
python -c "import starfix; print('ok')"
```

- [ ] **Verify GitHub Release was created**

```bash
gh release view v0.1.0
```

Expected: release exists with attached wheel and sdist.
