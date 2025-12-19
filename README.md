# CSCS HPC Storage Proxy

## Overview

The **CSCS HPC Storage Proxy** is a specialized middleware application designed to bridge the gap between **Waldur Mastermind** (a cloud brokerage platform) and high-performance computing (HPC) storage systems (like Lustre or GPFS).

Its primary function is to:

1. **Fetch** abstract storage resource definitions from Waldur.
2. **Enrich** them with low-level identity data (Unix UIDs/GIDs) from an internal HPC User API.
3. **Transform** the data into a strict, hierarchical JSON format required by storage backend provisioners.
4. **Calculate** filesystem quotas based on business logic.

## Architecture

The application is built on **FastAPI** and operates as a stateless translation layer.

```mermaid
graph TD
    subgraph "External Clients"
        Client[Storage Provisioner / Script]
    end

    subgraph "Storage Proxy (This App)"
        API[FastAPI Endpoint]
        Auth[Keycloak Middleware]
        Orch[Orchestrator]
        Map[Resource Mapper]
        Cache[In-Memory GID Cache]
    end

    subgraph "Upstream Services"
        Waldur[Waldur Mastermind API]
        HPC[CSCS HPC User API]
        IDP[Keycloak / OIDC Provider]
    end

    Client -->|GET /resources + Token| API
    API -->|Validate Token| Auth
    Auth <-->|Verify| IDP
    API --> Orch
    Orch -->|1. List Resources| Waldur
    Orch -->|2. Resolve GIDs| HPC
    HPC -->|OIDC Client Creds| IDP
    HPC -.->|Return GID| Cache
    Orch -->|3. Map & Calc Quotas| Map
    Map -->|JSON Response| Client
```

---

## 3. Installation

### Prerequisites

* Python 3.11+
* Access to a Waldur Mastermind instance.
* Access to the CSCS HPC User API (for GID resolution).
* A Keycloak/OIDC provider.

### Installation Steps

1. **Install via Pip**:

    ```bash
    pip install waldur-cscs-hpc-storage
    ```

2. **Or Install from Source**:

    ```bash
    git clone https://github.com/waldur/waldur-cscs-hpc-storage.git
    cd waldur-cscs-hpc-storage
    pip install .
    ```

---

## 4. Configuration

The application uses **Pydantic Settings**. Configuration is loaded in the following priority order:

1. **Environment Variables**
2. **Default Values**

Set variables in the environment or use a `.env` file (e.g., via Docker Compose or Kubernetes Secrets).

### 4.2. Detailed Configuration Reference

#### A. General Settings

| Env Variable      | Type | Default      | Description                                                           |
| :---------------- | :--- | :----------- | :-------------------------------------------------------------------- |
| `DEBUG`           | Bool | `False`      | Enables verbose logging.                                              |
| `STORAGE_SYSTEMS` | Dict | **Required** | Maps the API `storage_system` parameter to Waldur **Offering Slugs**. |

**Example:**

```bash
STORAGE_SYSTEMS='{"capstor": "cscs-capstor-offering", "vast": "cscs-vast-offering"}'
```

#### B. Waldur API (Source of Truth)

| Env Variable         | Default      | Description                                        |
| :------------------- | :----------- | :------------------------------------------------- |
| `WALDUR_API_URL`     | **Required** | Full URL to Waldur API.                            |
| `WALDUR_API_TOKEN`   | **Required** | Token for a Service Provider user in Waldur.       |
| `WALDUR_VERIFY_SSL`  | `True`       | Validate SSL certificates.                         |
| `WALDUR_SOCKS_PROXY` | `None`       | Optional SOCKS proxy (e.g., `socks5://host:port`). |

#### C. Authentication (Incoming Security)

Controls how clients authenticate *to this proxy*.

| Env Variable              | Default | Description                              |
| :------------------------ | :------ | :--------------------------------------- |
| `DISABLE_AUTH`            | `False` | **DANGER**: Disables all auth. Dev only. |
| `CSCS_KEYCLOAK_URL`       | `...`   | Base URL of Keycloak.                    |
| `CSCS_KEYCLOAK_REALM`     | `cscs`       | The Realm to validate tokens against.    |
| `CSCS_KEYCLOAK_CLIENT_ID` | **Required** | Client ID to validate `aud` claim.       |
| `CSCS_KEYCLOAK_CLIENT_SECRET` | **Required** | Client Secret for intospection.      |

#### D. HPC User API (Identity Resolution)

Used to fetch Unix GIDs for projects. This service authenticates itself to the API using OIDC Client Credentials.

| Env Variable              | Description                                                                   |
| :------------------------ | :---------------------------------------------------------------------------- |
| `HPC_USER_API_URL`        | Base URL of the User/GID service.                                             |
| `HPC_USER_CLIENT_ID`      | Client ID for machine-to-machine auth.                                        |
| `HPC_USER_CLIENT_SECRET`  | Client Secret for machine-to-machine auth.                                    |
| `HPC_USER_OIDC_TOKEN_URL` | Endpoint to fetch the machine token.                                          |
| `HPC_USER_SOCKS_PROXY`    | Specific SOCKS proxy for this connection.                                     |
| `HPC_USER_DEVELOPMENT_MODE` | If `True`, generates mock GIDs instead of crashing if the API is unreachable. |

#### E. Backend Logic (Quotas & Filesystem)

| Env Variable             | Default     | Description                                                  |
| :----------------------- | :---------- | :----------------------------------------------------------- |
| `STORAGE_FILE_SYSTEM`    | `lustre`    | The string identifier returned in JSON (e.g., GPFS, LUSTRE). |
| `INODE_BASE_MULTIPLIER`  | `1,000,000` | How many inodes per TB of storage.                           |
| `INODE_SOFT_COEFFICIENT` | `1.33`      | Multiplier for Soft Quota.                                   |
| `INODE_HARD_COEFFICIENT` | `2.0`       | Multiplier for Hard Quota.                                   |

*Validation Rule:* `inode_hard_coefficient` must be greater than `inode_soft_coefficient`.

#### F. Sentry (Error Tracking)

| Env Variable                | Description                          |
| :-------------------------- | :----------------------------------- |
| `SENTRY_DSN`                | The Sentry DSN URL.                  |
| `SENTRY_ENVIRONMENT`        | Tag (e.g., `production`).            |
| `SENTRY_TRACES_SAMPLE_RATE` | 0.0 to 1.0 (Performance monitoring). |

---

## 5. Operational Logic

### 5.1. The Hierarchy Builder

The proxy does not just list projects. It reconstructs a directory tree based on Waldur metadata.

1. **Tenant (Top Level)**: Maps to the Waldur **Offering Provider**.
    * Path: `/{system}/{type}/{provider_slug}`
2. **Customer (Mid Level)**: Maps to the Waldur **Resource Customer**.
    * Path: `.../{provider_slug}/{customer_slug}`
3. **Project (Leaf Level)**: Maps to the Waldur **Resource Project**.
    * Path: `.../{customer_slug}/{project_slug}`

### 5.2. Identity Resolution (GID Fetching)

The proxy cannot create storage unless it knows the **Unix GID** associated with the project.

1. **Check Cache**: An in-memory cache stores GIDs (Key: Project Slug) to reduce API load.
2. **API Call**: If missing, calls `HPC_USER_API_URL/api/v1/export/waldur/projects`.
3. **Token Refresh**: Handles OIDC token lifecycle automatically (fetches new token 5 minutes before expiry).
4. **Error Handling**:
    * *Production Mode*: If GID is not found, the resource **fails to process** (raises `MissingIdentityError`).
    * *Development Mode*: Generates a deterministic mock GID (`30000 + hash(slug) % 10000`).

### 5.3. Quota Calculation Formula

Quotas are derived from the storage limit (in TB) set in Waldur.

1. **Base Inodes** = `Storage_TB` * `inode_base_multiplier`
2. **Soft Inode Limit** = `Base Inodes` * `inode_soft_coefficient`
3. **Hard Inode Limit** = `Base Inodes` * `inode_hard_coefficient`

*Note: Administrative overrides in Waldur (Resource Options) take precedence over these calculations.*

---

## 6. API Reference

This section provides comprehensive documentation for the Storage Proxy API endpoints.

### 6.1. Authentication

All API endpoints require authentication using an OIDC token unless `DISABLE_AUTH=true` is set in development mode.

**Authentication Header:**

```
Authorization: Bearer <valid_oidc_token>
```

**Development Mode:**
* Set `DISABLE_AUTH=true` to bypass authentication
* Useful for local testing and integration development

---

### 6.2. GET `/api/storage-resources/`

Retrieves a paginated list of storage resources with optional filtering.

#### Request

**Headers:**
* `Authorization`: `Bearer <valid_oidc_token>` (required unless auth disabled)

**Query Parameters:**

| Parameter           | Type    | Required | Default | Description                                                       |
|:--------------------|:--------|:---------|:--------|:------------------------------------------------------------------|
| `storage_system`    | String  | No       | -       | Filter by storage system: `capstor`, `vast`, or `iopsstor`        |
| `data_type`         | String  | No       | -       | Filter by data type: `store`, `scratch`, `archive`, or `users`    |
| `status`            | String  | No       | -       | Filter by status: `active`, `pending`, `updating`, `removing`, `error` |
| `state`             | String  | No       | -       | Filter by Waldur state: `Creating`, `OK`, `Erred`, `Updating`, `Terminating` |
| `page`              | Integer | No       | 1       | Page number for pagination (starts at 1)                          |
| `page_size`         | Integer | No       | 100     | Number of items per page (max: 500)                               |

**Example Requests:**

```bash
# Get all storage resources
GET /api/storage-resources/

# Filter by storage system and data type
GET /api/storage-resources/?storage_system=capstor&data_type=store

# Paginate results
GET /api/storage-resources/?page=2&page_size=50

# Filter by status
GET /api/storage-resources/?status=active
```

#### Response

**Success Response (200 OK):**

```json
{
  "status": "success",
  "resources": [
    {
      "itemId": "a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
      "status": "active",
      "parentItemId": "f1e2d3c4-b5a6-4978-8c9d-0e1f2a3b4c5d",
      "mountPoint": {
        "default": "/capstor/store/cscs/customer-slug/project-slug"
      },
      "permission": {
        "value": "2770",
        "permissionType": "octal"
      },
      "storageSystem": {
        "itemId": "4b4a996a-8d6b-556d-ad60-202cefa6ecc3",
        "key": "capstor",
        "name": "CAPSTOR",
        "active": true
      },
      "storageFileSystem": {
        "itemId": "a04204cf-e3bf-5eb6-8323-0f3121afdd3b",
        "key": "lustre",
        "name": "LUSTRE",
        "active": true
      },
      "storageDataType": {
        "itemId": "6cea66c5-3133-54e1-9e5d-469deb675ceb",
        "key": "store",
        "name": "STORE",
        "active": true,
        "path": "store"
      },
      "target": {
        "targetType": "project",
        "targetItem": {
          "itemId": "4fab79ed-2eef-5acc-a504-1170e6518736",
          "key": "project-slug",
          "name": "Project Name",
          "unixGid": 30500,
          "status": "active",
          "active": true
        }
      },
      "quotas": [
        {
          "type": "space",
          "quota": 10.0,
          "unit": "tera",
          "enforcementType": "hard"
        },
        {
          "type": "space",
          "quota": 10.0,
          "unit": "tera",
          "enforcementType": "soft"
        },
        {
          "type": "inodes",
          "quota": 10000000.0,
          "unit": "none",
          "enforcementType": "hard"
        },
        {
          "type": "inodes",
          "quota": 7500000.0,
          "unit": "none",
          "enforcementType": "soft"
        }
      ]
    }
  ],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_count": 42,
    "total_pages": 1
  }
}
```

**Empty Results (200 OK):**

```json
{
  "status": "success",
  "resources": [],
  "pagination": {
    "page": 1,
    "page_size": 100,
    "total_count": 0,
    "total_pages": 0
  }
}
```

#### Response Fields

##### Root Object

| Field        | Type   | Description                                    |
|:-------------|:-------|:-----------------------------------------------|
| `status`     | String | Always `"success"` for successful requests     |
| `resources`  | Array  | List of storage resource objects              |
| `pagination` | Object | Pagination metadata                            |

##### Resource Object

| Field                  | Type   | Description                                          |
|:-----------------------|:-------|:-----------------------------------------------------|
| `itemId`               | UUID   | Unique identifier for this storage resource          |
| `status`               | String | Resource status: `active`, `pending`, `updating`, `removing`, `error` |
| `parentItemId`         | UUID   | UUID of parent resource (customer) or `null`         |
| `mountPoint`           | Object | Mount point information                              |
| `permission`           | Object | Permission settings                                  |
| `storageSystem`        | Object | Storage system metadata                              |
| `storageFileSystem`    | Object | File system metadata                                 |
| `storageDataType`      | Object | Data type metadata                                   |
| `target`               | Object | Target information (project/user/tenant/customer)    |
| `quotas`               | Array  | Current quota settings (4 items: 2 space + 2 inodes) |
| `oldQuotas`            | Array  | Previous quotas (only present during updates)        |
| `newQuotas`            | Array  | New quotas (only present during updates)             |

##### Additional Fields (When Available)

These fields appear only when there's an active order/action:

| Field                      | Type   | Description                                    |
|:---------------------------|:-------|:-----------------------------------------------|
| `approve_by_provider_url`  | String | URL to approve the pending action              |
| `reject_by_provider_url`   | String | URL to reject the pending action               |
| `set_state_done_url`       | String | URL to mark provisioning as complete           |
| `set_backend_id_url`       | String | URL to set the backend identifier              |

##### StorageItem Object (system/filesystem/datatype)

| Field    | Type    | Description                               |
|:---------|:--------|:------------------------------------------|
| `itemId` | UUID    | Deterministic UUID for this item          |
| `key`    | String  | Lowercase identifier (e.g., `"capstor"`)  |
| `name`   | String  | Uppercase display name (e.g., `"CAPSTOR"`) |
| `active` | Boolean | Whether this item is active               |
| `path`   | String  | Path prefix (only for `storageDataType`)  |

##### Target Object

| Field        | Type   | Description                                          |
|:-------------|:-------|:-----------------------------------------------------|
| `targetType` | String | Type of target: `project`, `user`, `tenant`, `customer` |
| `targetItem` | Object | Target-specific metadata (varies by type)            |

##### TargetItem Object (Project)

| Field      | Type    | Description                              |
|:-----------|:--------|:-----------------------------------------|
| `itemId`   | UUID    | Deterministic UUID for this project      |
| `key`      | String  | Project key/slug                         |
| `name`     | String  | Project name                             |
| `unixGid`  | Integer | UNIX group ID for the project            |
| `status`   | String  | Project status                           |
| `active`   | Boolean | Whether project is active                |

##### TargetItem Object (User)

| Field            | Type    | Description                       |
|:-----------------|:--------|:----------------------------------|
| `itemId`         | UUID    | Deterministic UUID for this user  |
| `key`            | String  | User key/slug                     |
| `name`           | String  | User name                         |
| `email`          | String  | User email address                |
| `unixUid`        | Integer | UNIX user ID                      |
| `status`         | String  | User status                       |
| `active`         | Boolean | Whether user is active            |
| `primaryProject` | Object  | Primary project information       |

##### Quota Object

| Field             | Type   | Description                                         |
|:------------------|:-------|:----------------------------------------------------|
| `type`            | String | Quota type: `space` or `inodes`                     |
| `quota`           | Float  | Quota value                                         |
| `unit`            | String | Unit: `tera` for space, `none` for inodes           |
| `enforcementType` | String | Enforcement level: `hard` or `soft`                 |

#### Error Responses

**Unauthorized (401):**

```json
{
  "detail": "Not authenticated"
}
```

**Forbidden (403):**

```json
{
  "detail": "Invalid or expired token"
}
```

**Bad Request (400):**

```json
{
  "detail": "Invalid parameter: page_size must be between 1 and 500"
}
```

**Internal Server Error (500):**

```json
{
  "detail": "Internal server error",
  "error": "Error message details"
}
```

---

### 6.3. Response Status Values

The `status` field in resources indicates the current lifecycle state:

| Status      | Description                                                  |
|:------------|:-------------------------------------------------------------|
| `pending`   | Resource is being created, waiting for provisioning          |
| `active`    | Resource is fully provisioned and operational                |
| `updating`  | Resource is being updated (e.g., quota change in progress)   |
| `removing`  | Resource is being terminated                                 |
| `removed`   | Resource has been successfully terminated                    |
| `error`     | Resource encountered an error during lifecycle operation     |

---

### 6.4. Filtering Examples

#### Filter by Storage System

```bash
GET /api/storage-resources/?storage_system=capstor
```

Returns only resources on the CAPSTOR storage system.

#### Filter by Data Type

```bash
GET /api/storage-resources/?data_type=users
```

Returns only user-type storage resources (home directories).

#### Combine Multiple Filters

```bash
GET /api/storage-resources/?storage_system=vast&data_type=scratch&status=active
```

Returns only active scratch storage on the VAST system.

#### Filter by Waldur State

```bash
GET /api/storage-resources/?state=Creating
```

Returns resources currently in the "Creating" state in Waldur.

---

### 6.5. Pagination

The API supports pagination to handle large result sets efficiently.

**Request Parameters:**
* `page`: Page number (starting from 1)
* `page_size`: Items per page (default: 100, maximum: 500)

**Response Pagination Object:**

```json
{
  "pagination": {
    "page": 2,
    "page_size": 50,
    "total_count": 156,
    "total_pages": 4
  }
}
```

**Example Navigation:**

```bash
# First page
GET /api/storage-resources/?page=1&page_size=50

# Next page
GET /api/storage-resources/?page=2&page_size=50

# Last page
GET /api/storage-resources/?page=4&page_size=50
```

---

## 7. Troubleshooting

### Common Error Codes

| Error                           | Cause                             | Solution                                                                                |
| :------------------------------ | :-------------------------------- | :-------------------------------------------------------------------------------------- |
| **500 ConfigurationError**      | Application settings are invalid. | Check server logs. Usually missing Env Vars or invalid coefficient logic.               |
| **502 UpstreamServiceError**    | Cannot contact Waldur or HPC API. | Check `WALDUR_API_URL`, network, or SOCKS proxy settings.                               |
| **401 Unauthorized**            | Token missing or invalid.         | Ensure your Bearer token is from the correct Realm and has `preferred_username`.        |
| **500 ResourceProcessingError** | `MissingIdentityError`            | The project exists in Waldur but not in the HPC User API. Ensure the project is synced. |

### Debugging

Enable debug logging to see the full mapping flow and API exchanges:

```bash
export DEBUG=true
```

To test without Authentication (Local Dev Only):

```bash
export DEVELOPMENT_MODE=true
export DISABLE_AUTH=true
export WALDUR_API_URL=http://127.0.0.1:8000/
export WALDUR_API_TOKEN=e38cd56f1ce5bf4ef35905f2bdcf84f1d7f2cc5e
export STORAGE_SYSTEMS='{"capstor": "capstor"}'
uv run uvicorn waldur_cscs_hpc_storage.api.main:app --host 127.0.0.1 --port 8080
```

### SOCKS Proxy Testing

If running behind a corporate firewall, ensure `httpx` (the underlying library used) can reach the SOCKS proxy.

```bash
export WALDUR_SOCKS_PROXY="socks5://localhost:1080"
```

Logs will indicate: `Using SOCKS proxy for API request...`

---

## 8. Integrator Guide

This section is for developers building **Storage Provisioners** (drivers) that consume this proxy's API.

### 8.1. Resource Lifecycle

The provisioner should run a reconciliation loop (e.g., every minute) that:

1. Calls to `GET /api/storage-resources/` to list all resources.
2. Iterates through the list and checks the `status` field.
3. Performs the necessary action (Create, Update, Delete) based on the status.
4. Reports back to Waldur via the `callback_url` (if provided) to finalize the state.

### 8.2. The 3-Step Provisioning Protocol

A complete provisioning flow involves three distinct callbacks to Waldur. This ensures the Order transitions correctly through its state machine:

1. **Approve/Reject** (`approve_by_provider_url`):
    * **When**: Order is in `PENDING_PROVIDER` state.
    * **Purpose**: Acknowledges the request. Moves Order to `EXECUTING`.
2. **Set State Done** (`set_state_done_url`):
    * **When**: Order is in `EXECUTING` state (provisioning finished).
    * **Purpose**: Confirms the work is complete. Moves Order to `DONE`.
3. **Report Quotas** (`update_resource_options_url`):
    * **When**: Order is in `EXECUTING` state (before `set_state_done`).
    * **Purpose**: Reports the actual applied quotas back to Waldur.
4. **Set Backend ID** (`set_backend_id_url`):
    * **When**: Order is in `DONE` state (or `EXECUTING`).
    * **Purpose**: Links the Waldur resource to the specific backend identifier.

**Note**: You can send `set_backend_id` earlier (during `EXECUTING`), but `set_state_done` **must** be called to finalize the order.

### 8.3. The Create Flow

**Trigger**: A user creates a resource in Waldur.
**Proxy Status**: `pending`

The JSON payload will look like this:

```json
{
  "itemId": "a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
  "status": "pending",
  "parentItemId": "f1e2d3c4-b5a6-4978-8c9d-0e1f2a3b4c5d",
  "mountPoint": {
    "default": "/capstor/store/cscs/customer-slug/project-slug"
  },
  "permission": {
    "value": "2770",
    "permissionType": "octal"
  },
  "storageSystem": {
    "itemId": "4b4a996a-8d6b-556d-ad60-202cefa6ecc3",
    "key": "capstor",
    "name": "CAPSTOR",
    "active": true
  },
  "storageFileSystem": {
    "itemId": "a04204cf-e3bf-5eb6-8323-0f3121afdd3b",
    "key": "lustre",
    "name": "LUSTRE",
    "active": true
  },
  "storageDataType": {
    "itemId": "6cea66c5-3133-54e1-9e5d-469deb675ceb",
    "key": "store",
    "name": "STORE",
    "active": true,
    "path": "store"
  },
  "target": {
    "targetType": "project",
    "targetItem": {
      "itemId": "4fab79ed-2eef-5acc-a504-1170e6518736",
      "key": "project-slug",
      "name": "Project Name",
      "unixGid": 30500,
      "status": "active",
      "active": true
    }
  },
  "quotas": [
    {
      "type": "space",
      "quota": 10.0,
      "unit": "tera",
      "enforcementType": "hard"
    },
    {
      "type": "space",
      "quota": 10.0,
      "unit": "tera",
      "enforcementType": "soft"
    },
    {
      "type": "inodes",
      "quota": 10000000.0,
      "unit": "none",
      "enforcementType": "hard"
    },
    {
      "type": "inodes",
      "quota": 7500000.0,
      "unit": "none",
      "enforcementType": "soft"
    }
  ],
  "approve_by_provider_url": "https://waldur.example.com/api/marketplace-orders/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/approve_by_provider/",
  "reject_by_provider_url": "https://waldur.example.com/api/marketplace-orders/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/reject_by_provider/",
  "set_state_done_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_state_done/",
  "set_backend_id_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_backend_id/",
  "update_resource_options_url": "https://waldur.example.com/api/marketplace-provider-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/update_options_direct/"
}
```

**Action**:

1. Create the filesystem directory/fileset at the path specified in `mountPoint.default`.
2. Set the Quotas (Space & Inodes from `quotas` - both soft and hard limits).
3. Set the Ownership (GID from `target.targetItem.unixGid`).
4. Set the Permissions (from `permission.value`).
5. **POST** to `approve_by_provider_url` (empty body) to acknowledge the request.
6. **POST** to `update_resource_options_url` with the applied quotas (e.g. `{"options": {"soft_quota_space": 10.0}}`).
7. **POST** to `set_state_done_url` (empty body) to signal completion.
8. **POST** to `set_backend_id_url` with the backend identifier.

Once approved, the resource state in Waldur becomes `OK`, and the proxy status becomes `active`.

### 8.4. The Update Flow

**Trigger**: A user changes the storage limit in Waldur.
**Proxy Status**: `updating`

The proxy provides both `oldQuotas` (current state) and `newQuotas` (desired state).

```json
{
  "itemId": "a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
  "status": "updating",
  "parentItemId": "f1e2d3c4-b5a6-4978-8c9d-0e1f2a3b4c5d",
  "mountPoint": {
    "default": "/capstor/store/cscs/customer-slug/project-slug"
  },
  "permission": {
    "value": "2770",
    "permissionType": "octal"
  },
  "storageSystem": {
    "itemId": "4b4a996a-8d6b-556d-ad60-202cefa6ecc3",
    "key": "capstor",
    "name": "CAPSTOR",
    "active": true
  },
  "storageFileSystem": {
    "itemId": "a04204cf-e3bf-5eb6-8323-0f3121afdd3b",
    "key": "lustre",
    "name": "LUSTRE",
    "active": true
  },
  "storageDataType": {
    "itemId": "6cea66c5-3133-54e1-9e5d-469deb675ceb",
    "key": "store",
    "name": "STORE",
    "active": true,
    "path": "store"
  },
  "target": {
    "targetType": "project",
    "targetItem": {
      "itemId": "4fab79ed-2eef-5acc-a504-1170e6518736",
      "key": "project-slug",
      "name": "Project Name",
      "unixGid": 30500,
      "status": "active",
      "active": true
    }
  },
  "quotas": [
    {
      "type": "space",
      "quota": 20.0,
      "unit": "tera",
      "enforcementType": "hard"
    },
    {
      "type": "space",
      "quota": 18.0,
      "unit": "tera",
      "enforcementType": "soft"
    },
    {
      "type": "inodes",
      "quota": 40000000.0,
      "unit": "none",
      "enforcementType": "hard"
    },
    {
      "type": "inodes",
      "quota": 26600000.0,
      "unit": "none",
      "enforcementType": "soft"
    }
  ],
  "oldQuotas": [
    {
      "type": "space",
      "quota": 10.0,
      "unit": "tera",
      "enforcementType": "hard"
    },
    {
      "type": "space",
      "quota": 9.0,
      "unit": "tera",
      "enforcementType": "soft"
    },
    {
      "type": "inodes",
      "quota": 20000000.0,
      "unit": "none",
      "enforcementType": "hard"
    },
    {
      "type": "inodes",
      "quota": 13300000.0,
      "unit": "none",
      "enforcementType": "soft"
    }
  ],
  "newQuotas": [
    {
      "type": "space",
      "quota": 20.0,
      "unit": "tera",
      "enforcementType": "hard"
    },
    {
      "type": "space",
      "quota": 18.0,
      "unit": "tera",
      "enforcementType": "soft"
    },
    {
      "type": "inodes",
      "quota": 40000000.0,
      "unit": "none",
      "enforcementType": "hard"
    },
    {
      "type": "inodes",
      "quota": 26600000.0,
      "unit": "none",
      "enforcementType": "soft"
    }
  ],
  "approve_by_provider_url": "https://waldur.example.com/api/marketplace-orders/b8c0d1e2-f3a4-5b6c-7d8e-9f0a1b2c3d4e/approve_by_provider/",
  "reject_by_provider_url": "https://waldur.example.com/api/marketplace-orders/b8c0d1e2-f3a4-5b6c-7d8e-9f0a1b2c3d4e/reject_by_provider/",
  "set_state_done_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_state_done/",
  "set_backend_id_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_backend_id/",
  "update_resource_options_url": "https://waldur.example.com/api/marketplace-provider-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/update_options_direct/"
}
```

**Action**:

1. Apply the new quotas from `newQuotas` to the directory at `mountPoint.default`.
2. **POST** to `approve_by_provider_url` (empty body) to acknowledge the request.
3. **POST** to `update_resource_options_url` with the applied quotas.
4. **POST** to `set_state_done_url` (empty body) to confirm the update.

### 8.5. The Terminate Flow

**Trigger**: A user deletes the resource in Waldur.
**Proxy Status**: `removing`

```json
{
  "itemId": "a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d",
  "status": "removing",
  "parentItemId": "f1e2d3c4-b5a6-4978-8c9d-0e1f2a3b4c5d",
  "mountPoint": {
    "default": "/capstor/store/cscs/customer-slug/project-slug"
  },
  "permission": {
    "value": "2770",
    "permissionType": "octal"
  },
  "storageSystem": {
    "itemId": "4b4a996a-8d6b-556d-ad60-202cefa6ecc3",
    "key": "capstor",
    "name": "CAPSTOR",
    "active": true
  },
  "storageFileSystem": {
    "itemId": "a04204cf-e3bf-5eb6-8323-0f3121afdd3b",
    "key": "lustre",
    "name": "LUSTRE",
    "active": true
  },
  "storageDataType": {
    "itemId": "6cea66c5-3133-54e1-9e5d-469deb675ceb",
    "key": "store",
    "name": "STORE",
    "active": true,
    "path": "store"
  },
  "target": {
    "targetType": "project",
    "targetItem": {
      "itemId": "4fab79ed-2eef-5acc-a504-1170e6518736",
      "key": "project-slug",
      "name": "Project Name",
      "unixGid": 30500,
      "status": "active",
      "active": true
    }
  },
  "quotas": [
    {
      "type": "space",
      "quota": 20.0,
      "unit": "tera",
      "enforcementType": "hard"
    },
    {
      "type": "space",
      "quota": 18.0,
      "unit": "tera",
      "enforcementType": "soft"
    },
    {
      "type": "inodes",
      "quota": 40000000.0,
      "unit": "none",
      "enforcementType": "hard"
    },
    {
      "type": "inodes",
      "quota": 26600000.0,
      "unit": "none",
      "enforcementType": "soft"
    }
  ],
  "approve_by_provider_url": "https://waldur.example.com/api/marketplace-orders/order-uuid/approve_by_provider/",
  "reject_by_provider_url": "https://waldur.example.com/api/marketplace-orders/order-uuid/reject_by_provider/",
  "set_state_done_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_state_done/",
  "set_backend_id_url": "https://waldur.example.com/api/marketplace-resources/a7b9c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d/set_backend_id/"
}
```

**Action**:

1. Archive or delete the data at the directory specified in `mountPoint.default`.
2. Remove any quota configuration for the filesystem.
3. **POST** to `approve_by_provider_url` (empty body) to acknowledge the request.
4. **POST** to `set_state_done_url` (empty body) to confirm the deletion.

### 8.6. Detailed Waldur Workflow

To better understand the lifecycle, it helps to know the underlying Waldur states.

#### Order States

Orders progress through a carefully managed state machine with approval workflows:

```mermaid
stateDiagram-v2
    [*] --> PENDING_CONSUMER : Order created

    PENDING_CONSUMER --> PENDING_PROVIDER : Consumer approves
    PENDING_CONSUMER --> PENDING_PROJECT : Consumer approves & project start date is future
    PENDING_CONSUMER --> PENDING_START_DATE : Consumer approves & no provider review & order start date is future
    PENDING_CONSUMER --> CANCELED : Consumer cancels
    PENDING_CONSUMER --> REJECTED : Consumer rejects

    PENDING_PROVIDER --> PENDING_START_DATE : Provider approves & order start date is future
    PENDING_PROVIDER --> EXECUTING : Provider approves
    PENDING_PROVIDER --> CANCELED : Provider cancels
    PENDING_PROVIDER --> REJECTED : Provider rejects

    PENDING_PROJECT --> PENDING_PROVIDER: Project activates & provider review needed
    PENDING_PROJECT --> PENDING_START_DATE: Project activates & no provider review & order start date is future
    PENDING_PROJECT --> EXECUTING: Project activates
    PENDING_PROJECT --> CANCELED : Project issues

    PENDING_START_DATE --> EXECUTING : Start date reached
    PENDING_START_DATE --> CANCELED : User cancels

    EXECUTING --> DONE : Processing complete
    EXECUTING --> ERRED : Processing failed

    DONE --> [*]
    ERRED --> [*]
    CANCELED --> [*]
    REJECTED --> [*]
```

#### Order State Descriptions

| State | Description | Triggers |
|-------|-------------|----------|
| **PENDING_CONSUMER** | Awaiting customer approval | Order creation |
| **PENDING_PROVIDER** | Awaiting service provider approval | Consumer approval |
| **PENDING_PROJECT** | Awaiting project activation | Provider approval |
| **PENDING_START_DATE** | Awaiting the order's specified start date. | Activation when a future start date is set on the order. |
| **EXECUTING** | Resource provisioning in progress | Processor execution |
| **DONE** | Order completed successfully | Resource provisioning success |
| **ERRED** | Order failed with errors | Processing errors |
| **CANCELED** | Order canceled by user/system | User cancellation |
| **REJECTED** | Order rejected by provider | Provider rejection |

#### Resource States

Resources maintain their own lifecycle independent of orders:

```mermaid
stateDiagram-v2
    [*] --> CREATING : Order approved

    CREATING --> OK : Provisioning success
    CREATING --> ERRED : Provisioning failed

    OK --> UPDATING : Update requested
    OK --> TERMINATING : Deletion requested

    UPDATING --> OK : Update success
    UPDATING --> ERRED : Update failed

    TERMINATING --> TERMINATED : Deletion success
    TERMINATING --> ERRED : Deletion failed

    ERRED --> OK : Error resolved
    ERRED --> UPDATING : Retry update
    ERRED --> TERMINATING : Force deletion

    TERMINATED --> [*]
```

#### Resource State Descriptions

| State | Description | Operations Allowed |
|-------|-------------|-------------------|
| **CREATING** | Resource being provisioned | Monitor progress |
| **OK** | Resource active and healthy | Update, delete, use |
| **UPDATING** | Resource being modified | Monitor progress |
| **TERMINATING** | Resource being deleted | Monitor progress |
| **TERMINATED** | Resource deleted | Archive, billing |
| **ERRED** | Resource in error state | Retry, investigate, delete |

### 8.7. Quota Reporting Format

When reporting quotas via the `update_resource_options_url`, the payload must map the internal quota representation to specific keys expected by Waldur.

**Mapping Table:**

| Quota Type | Enforcement Type | Mapped Key | Unit |
| :--- | :--- | :--- | :--- |
| `space` | `soft` | `soft_quota_space` | TB |
| `space` | `hard` | `hard_quota_space` | TB |
| `inodes` | `soft` | `soft_quota_inodes` | Count |
| `inodes` | `hard` | `hard_quota_inodes` | Count |

**Example Payload:**

```json
{
  "options": {
    "soft_quota_space": 10.0,
    "hard_quota_space": 12.0,
    "soft_quota_inodes": 1000000,
    "hard_quota_inodes": 1500000
  }
}
```
