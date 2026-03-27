# Changelog

## 0.8.1 - 2026-03-27

### Highlights

This is a patch release that fixes a CI reliability issue where the release script produced malformed changelog files, causing pre-commit hooks to fail.

### Bug Fixes

- **Release script**: Fix trailing blank line in generated CHANGELOG.md that caused the `end-of-file-fixer` pre-commit hook to fail in CI.

### Statistics

> 1 commit, 2 files changed (+5/-4 lines)

---

## 0.8.0 - 2026-03-27

### Highlights

This is the first formal release of the CSCS HPC Storage Proxy, representing a ground-up rebuild of the service architecture. The codebase has been restructured into a clean service-oriented design with proper dependency injection, Pydantic-based configuration validation, and async operations throughout. Provisioners now receive richer callback URLs, accurate quota calculations, and hierarchical resource data with proper pagination.

### What's New

- **API**: Include package version in the API response payload.
- **Mapper**: Use Waldur `backend_id` as mount point path when set, replacing generated paths.
- **GID**: Batch-resolve Unix GIDs for all project slugs upfront, reducing HPC User API calls per request.
- **Schemas**: Add `set_state_ok_url` callback for marketplace provider resources.
- **Schemas**: Restore `update_resource_options_url` for pending provider orders so provisioners can update resource options during approval.
- **Pagination**: Add `total_count` parameter and `has_next` flag to paginated responses.
- **Schemas**: Conditionally override resource state to OK when a resource is updating/terminating with a pending consumer order.
- **Schemas**: Expose old and new quotas for resource update orders.
- **Quota**: Introduce centralized `QuotaCalculator` service with configurable inode multipliers and coefficients.
- **Config**: Migrate to Pydantic `BaseSettings` with full environment variable support, replacing YAML configuration.
- **Config**: Add strong validation for URLs, API tokens, auth settings, and SOCKS proxy configuration.
- **Config**: Mask sensitive values in configuration logging output.
- **Serialization**: Add custom JSON serialization for UUID and Enum types.
- **Error handling**: Implement structured exception hierarchy (`StorageProxyError`, `UpstreamServiceError`, `ResourceProcessingError`, `ConfigurationError`, `MissingIdentityError`) with dedicated API handlers.
- **GID**: Add mock GID service with development mode fallback when HPC User API is unavailable.
- **Hierarchy**: Introduce `HierarchyBuilder` for constructing tenant/customer/project resource structure.
- **Observability**: Integrate Sentry SDK for error tracking and performance monitoring.
- **Release**: Add automated release and changelog generation scripts.

### Improvements

- **Architecture**: Restructure into `api/`, `services/`, `mapper/`, `models/`, and `config/` packages with clear separation of concerns.
- **Services**: Convert GID service, orchestrator, and Waldur service to fully async operations.
- **Dependencies**: Use singleton service instances via dependency injection, reused across requests.
- **Quota**: Derive soft space quota directly instead of requiring a separate `soft_quota_space` option.
- **Waldur**: Fetch all resources and push status filter as Waldur state parameter for correct server-side filtering.
- **Platform**: Drop Python 3.10 support; minimum version is now Python 3.11.

### Bug Fixes

- **Mapper**: Correct Waldur resource state mapping for erred and terminated resources.
- **Waldur**: Fix state filter to accept lists and exclude early pending orders.
- **Schemas**: Fix `backend_id` URL construction.

### Statistics

> 179 commits, 76 files changed (+9852/−6724 lines)

---
