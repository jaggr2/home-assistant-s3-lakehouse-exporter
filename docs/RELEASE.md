# Release runbook

Maintainer checklist for cutting, verifying, and rolling back a release of the
Home Assistant S3 Lakehouse Exporter add-on.

## Release rules

A release exists only when **both** of the following are true:

- `home-assistant-s3-lakehouse-exporter/config.yaml` has a new `version:`.
- `CHANGELOG.md` has a matching `## [X.Y.Z]` entry.

Add-on store users receive updates from the `version:` bump alone. CI enforces
both rules on every `v*` tag and fails the release job otherwise.

Versioning follows [SemVer](https://semver.org/):

- **patch** — fixes only.
- **minor** — new features or new (optional) config options.
- **major** — breaking changes. Within `1.x` there must be **no breaking
  config changes**: new options ship optional, with working defaults.

## Pre-flight checklist

- [ ] `main` CI is green: `lint-test` (ruff + unit tests), `integration` (the
      S3 suite against a SeaweedFS service container), and the `docker`
      image-build job for both arches.
- [ ] `CHANGELOG.md` has a `## [X.Y.Z]` entry describing user-visible changes.
- [ ] `home-assistant-s3-lakehouse-exporter/config.yaml` `version:` is `X.Y.Z`.
- [ ] README matches shipped behavior (config reference, IAM policy, health
      output, querying examples).
- [ ] All dependencies pinned in `requirements.txt` / `requirements-dev.txt`.
- [ ] Local integration run is green:
      `docker compose up -d`, then the `pytest -m integration` invocation from
      the README's Development section.
- [ ] Quick local image build works:
      `docker build -t test home-assistant-s3-lakehouse-exporter`
- [ ] (Recommended) dogfooding sign-off complete — see `docs/dogfooding.md`.

## Cutting the release

```sh
git tag vX.Y.Z && git push origin vX.Y.Z
```

Watch the `CI` workflow. On a `v*` tag, two jobs run after `lint-test` and
`integration`:

1. **`release`** (matrix over `amd64` and `aarch64`):
   - verifies the tag equals the `version:` in `config.yaml`, and that
     `CHANGELOG.md` contains a `## [X.Y.Z]` heading;
   - builds and pushes `ghcr.io/jaggr2/home-assistant-s3-lakehouse-exporter:
     vX.Y.Z-amd64` and `...:vX.Y.Z-aarch64`.
2. **`release-manifest`** assembles the single multi-arch manifest
   `ghcr.io/jaggr2/home-assistant-s3-lakehouse-exporter:vX.Y.Z` and re-points
   `:latest` at it. This manifest is what `config.yaml`'s `image:` reference
   pulls — no `{arch}` placeholder.

## Post-release verification

- [ ] Both platforms are in the manifest:
      `docker buildx imagetools inspect ghcr.io/jaggr2/home-assistant-s3-lakehouse-exporter:vX.Y.Z`
      lists `linux/amd64` and `linux/arm64`.
- [ ] On a real Home Assistant box: **Settings → Add-ons → Add-on store** shows
      the update; install/upgrade, then start the add-on.
- [ ] The Log tab shows `Home Assistant S3 Lakehouse Exporter X.Y.Z starting`.
- [ ] `http://<host>:8099/health` returns `"status": "ok"`.
- [ ] Fresh data lands in the bucket (steps in `docs/dogfooding.md`).

## Fallback: Supervisor cannot pull a manifest list

If the Supervisor refuses the multi-arch manifest (install fails with an image
pull error), fall back to per-arch repositories:

1. Push per-arch images as
   `ghcr.io/jaggr2/{arch}-home-assistant-s3-lakehouse-exporter:vX.Y.Z`,
   where `{arch}` is `amd64` or `aarch64`.
2. Change `config.yaml`'s `image:` to the placeholder form:
   `image: "ghcr.io/jaggr2/{arch}-home-assistant-s3-lakehouse-exporter"`
   The Supervisor substitutes `{arch}` per host architecture.
3. Ship that change as a normal release (version bump + CHANGELOG entry).

## Rollback

- **Users**: select the previous version in the add-on's version picker and
  restart — the Supervisor keeps prior images.
- **Maintainers**: never overwrite or delete a released `:vX.Y.Z` tag. Fix
  forward with a new version. Re-point `:latest` only to a fixed release; the
  `release-manifest` job does this on the next tag.

## One-time repository setup

- [ ] Create the GitHub repository and push `main`.
- [ ] Enable Renovate for the repository — `renovate.json` is included and
      covers the base image (pinned digests) and Python dependencies.
- [ ] GHCR auth in CI is automatic: the workflow's `GITHUB_TOKEN` carries
      `packages: write` for the release jobs.
- [ ] After the first image push, set the GHCR package visibility to
      **public** (the manifest package and, if used, the per-arch fallback
      packages) — otherwise add-on installs fail with an auth error.
