# Qiita Architecture

Qiita is a scalable multi-omic study management, processing, and analysis platform for microbiome data (amplicon, metagenomic, metatranscriptomic, metabolomic, proteomic). Schema informed by existing Qiita data model (carried forward surgically by human guidance) and BioSample (scope TBD).

**Scale:** Millions of samples, 100s of TB of data.

## System Architecture

```mermaid
graph TB
    subgraph clients ["Client Interfaces (TBD)"]
        CLI["CLI Client"]
        WEB["Web Application"]
        SDK["Python/R SDK"]
        NOTEBOOK["Notebook Integration"]
    end

    subgraph gateway ["API Gateway"]
        NX["nginx<br/>TLS termination<br/>REST + gRPC routing"]
    end

    subgraph auth ["Authentication"]
        AR["AuthRocket<br/>OAuth2 / JWT"]
    end

    subgraph services ["Core Services"]
        CP["qiita-control-plane<br/>━━━━━━━━━━━━━━━━━━━<br/>FastAPI (Python)<br/>REST API<br/>━━━━━━━━━━━━━━━━━━━<br/>Study/Sample/Prep CRUD<br/>Search<br/>Work ticket management<br/>Flight ticket signing<br/>File registration orchestration"]
        DP["qiita-data-plane (N instances)<br/>━━━━━━━━━━━━━━━━━━━<br/>Arrow Flight (Rust)<br/>gRPC<br/>━━━━━━━━━━━━━━━━━━━<br/>DoGet / DoPut / DoAction<br/>DuckDB + DuckLake<br/>Parquet file registration"]
        CO["qiita-compute-orchestrator<br/>━━━━━━━━━━━━━━━━━━━<br/>Python service<br/>━━━━━━━━━━━━━━━━━━━<br/>Job lifecycle management<br/>slurmrestd client<br/>Output verification<br/>Log collection"]
    end

    subgraph shared_lib ["Shared Library"]
        COMMON["qiita-common<br/>━━━━━━━━━━━━━━━━━━━<br/>Pydantic models<br/>Config patterns<br/>REST client utilities"]
    end

    subgraph compute ["HPC Compute"]
        SR["slurmrestd<br/>SLURM REST API"]
        SL["SLURM Cluster<br/>━━━━━━━━━━━━━━━━━━━<br/>Apptainer/Singularity<br/>Containerized workflows"]
    end

    subgraph storage ["Data Storage"]
        PG_APP["Postgres (App DB)<br/>━━━━━━━━━━━━━━━━━━━<br/>Users, roles, studies<br/>Samples, preparations<br/>Work tickets, provenance"]
        PG_DL["Postgres (DuckLake Catalog)<br/>━━━━━━━━━━━━━━━━━━━<br/>Snapshots, data files<br/>Schemas, partitions"]
        FS["Shared Filesystem<br/>━━━━━━━━━━━━━━━━━━━<br/>/data/staging/ (raw uploads)<br/>/data/results/ (Parquet)<br/>/data/logs/ (job logs)"]
    end

    %% Client connections
    CLI -->|REST + gRPC| NX
    WEB -->|REST| NX
    SDK -->|REST + gRPC| NX
    NOTEBOOK -->|REST + gRPC| NX

    %% Auth
    clients -.->|OAuth2 login| AR
    AR -.->|JWKS public keys| CP
    AR -.->|JWKS public keys| DP

    %% Gateway routing
    NX -->|REST| CP
    NX -->|gRPC / HTTP/2| DP

    %% Service interactions
    CP -->|"submit job"| CO
    CO -->|"job status callback"| CP
    DP -->|"upload complete callback"| CP
    CP -->|"register file"| DP

    %% Shared library
    COMMON -.->|"path dependency"| CP
    COMMON -.->|"path dependency"| CO

    %% Compute
    CO -->|"submit/poll via REST"| SR
    SR -->|"schedule"| SL

    %% Storage
    CP -->|asyncpg| PG_APP
    DP -->|DuckDB| PG_DL
    DP -->|"read/write Parquet"| FS
    SL -->|"read input,<br/>write output + logs"| FS

    %% Styling
    classDef tbd fill:#f9f0ff,stroke:#9b59b6,stroke-width:2px,stroke-dasharray: 5 5
    classDef service fill:#e8f4fd,stroke:#2980b9,stroke-width:2px
    classDef storage fill:#fdf2e9,stroke:#e67e22,stroke-width:2px
    classDef compute fill:#eafaf1,stroke:#27ae60,stroke-width:2px
    classDef gateway fill:#fdebd0,stroke:#d35400,stroke-width:2px
    classDef auth fill:#f5eef8,stroke:#8e44ad,stroke-width:2px
    classDef lib fill:#f0f0f0,stroke:#7f8c8d,stroke-width:1px,stroke-dasharray: 3 3

    class CLI,WEB,SDK,NOTEBOOK tbd
    class CP,DP,CO service
    class PG_APP,PG_DL,FS storage
    class SR,SL compute
    class NX gateway
    class AR auth
    class COMMON lib
```

**Legend:**
- **Solid lines** — runtime data/request flow
- **Dashed lines** — configuration/build-time dependencies
- **Purple dashed border** — Client interfaces (unresolved, to be discussed)

## Components

- **qiita-control-plane** — Client-facing REST API (Python 3.14, FastAPI, asyncpg, Postgres, dbmate, OpenAPI, PyTest, ruff, uv, GitHub Actions CI). Handles CRUD for study/sample/preparation, search, work ticket creation/management. Signs Flight tickets (HMAC-SHA256) for client access to data plane. Orchestrates file registration in DuckLake (via data plane) after compute completion.
- **qiita-data-plane** — Data layer (Rust, arrow-flight, DuckDB v1.5.1, duckdb-miint extension, DuckLake w/ Postgres catalog). Arrow Flight protocol (gRPC-based). Intentionally "dumb" — select/insert/delete by exact integer identifiers. Clients connect directly through nginx. Verifies JWTs (AuthRocket JWKS) and Flight ticket signatures (HMAC-SHA256). Registers Parquet files into DuckLake via `ducklake_add_data_files` (metadata-only, no I/O). Calls back to control plane REST endpoint on completion/failure. Runs as dedicated `qiita-data-plane` system user; verifies result file permissions before registration and rejects files that are not `440`. **Horizontally scalable**: each instance holds an independent DuckDB+DuckLake connection to the shared Postgres catalog; DuckLake's snapshot-isolated concurrent read model means multiple instances never block each other. nginx load-balances gRPC traffic across all instances.
- **qiita-compute-orchestrator** — Separate Python service for compute lifecycle management. Owns the full job lifecycle: submit via slurmrestd, poll for status, detect completion/failure, verify output, collect logs, and report back to control plane. SLURM jobs are truly dumb (read input, process, write output, exit). Abstracts compute backend behind a clean `ComputeBackend` interface (SLURM primary, future offload secondary).
- **qiita-common** — Shared Python library for control plane and compute orchestrator. Pydantic models (work ticket states, API request/response schemas), config patterns, and REST client utilities. Prevents drift between services' understanding of the API contract.
- **API gateway** — nginx: REST to qiita-control-plane, Arrow Flight/gRPC (HTTP/2+TLS) load-balanced across N qiita-data-plane instances.
- **Auth** — OAuth2 via AuthRocket, local user/role tables in Postgres for access control. JWT verified locally at both services via cached JWKS public keys. Dedicated `compute` service account for compute orchestrator callbacks (narrow permissions: update work tickets, request file registration only). Dedicated `data-plane` service account for data plane callbacks.
- **Client interfaces** — **[UNRESOLVED]** How users interact with Qiita. Placeholder candidates include: CLI tool, web application, Python/R SDK, and notebook integration. Details on which interfaces to build, their scope, and priorities are TBD. All client interfaces connect through nginx and authenticate via AuthRocket. REST-only clients interact with the control plane; clients needing bulk data transfer also use Arrow Flight (gRPC) to the data plane.

## Data Model

### Identifier Hierarchy

All identifiers are uint64, minted exclusively by the control plane. The data plane treats all identifiers as opaque integers.

- **`study_idx`** — unique identifier for a study. A study is a logical collection of samples and preparations, including meta-analyses that group `prep_sample_idx` values from other studies without uploading new data.
- **`prep_idx`** — unique identifier for a preparation (a set of observed data, e.g., a sequencing run). Belongs to one study.
- **`sample_idx`** — unique identifier for a physical sample. A study has one-to-many samples. A sample can appear across multiple studies (e.g., shared controls, meta-analysis groupings).
- **`prep_sample_idx`** — unique identifier for an instance of a physical sample on a preparation. A prep can carry the same physical sample multiple times (technical or biological replicates); a sample can appear on multiple preps. `prep_sample_idx` is the finest-grained unit of raw input data.
- **`processing_idx`** — unique identifier for a processing method: a specific `(workflow_name, workflow_version, parameter_set)` combination. Immutable once created. Reusable across preps and studies. Opaque integer in the data plane; detail lives in the control plane `processing_methods` table.
- **`processed_prep_sample_idx`** — unique identifier for the result of applying a `processing_idx` to a `prep_sample_idx`. Minted by the control plane before job submission. Processing cannot create new samples — all `processed_prep_sample_idx` values for a job are pre-assigned from the known `prep_sample_idx` set on the prep.

### Processing Methods

`processing_idx` detail lives in the control plane `processing_methods` table:

| Column | Type | Notes |
|---|---|---|
| `processing_idx` | uint64 PK | |
| `workflow_name` | text | |
| `workflow_version` | text | |
| `parameters_hash` | text | SHA-256 of canonical JSON parameters; deduplication key |
| `parameters_jsonb` | jsonb | full parameter set |
| `created_at` | timestamp | |
| `created_by` | text | |

Two submissions with identical `(workflow_name, workflow_version, parameters)` resolve to the same `processing_idx` via `parameters_hash` — the control plane upserts on this key rather than minting a duplicate.

### Identifier Columns in Parquet

All result Parquet files must include these columns, in this order, and be sorted by them:

```
study_idx  prep_idx  sample_idx  prep_sample_idx  processing_idx  processed_prep_sample_idx
```

This sort order provides two compounding layers of query optimisation:

- **DuckLake catalog** (`ducklake_file_column_stats`): min/max per column per file stored in Postgres. Any query filtering on identifier columns prunes whole files before DuckLake opens anything.
- **Parquet row group statistics**: sorted data produces tight, non-overlapping min/max ranges per row group — predicate pushdown skips row groups with zero false positives for point lookups and range scans. Without sorting, row group ranges overlap and pushdown degrades to near-useless for selective queries.

The sort is enforced in the reduce step (see Compute Orchestrator), so it is consistent regardless of what the map phase produces.

### Sample Metadata

Sample metadata — descriptive attributes of physical samples (specimen type, collection site, host age, treatment status, environmental parameters, etc.) — lives exclusively in the control plane (Postgres app DB) as attributes on `sample_idx` and related entities. It does not exist in the data plane.

Reasons:
- Metadata is structured relational data tightly coupled to the identifier model already in the control plane
- Schema validation against BioSample / MIMARKS compliance requirements is enforced at insert time
- Metadata and processed measurement data have different update semantics — a metadata correction (e.g., fixing a mislabelled collection site) does not invalidate or require reprocessing of any Parquet files in the data plane
- Access control decisions are already made in the control plane; metadata-driven filtering is a natural extension of the same authorization layer

**Search pattern:** the control plane exposes search and filter endpoints over metadata. A client submits a query (e.g., "fecal samples from antibiotic-naive subjects in study X"), receives the authorized set of `prep_sample_idx` (or `processed_prep_sample_idx`) identifiers matching the criteria, and uses those IDs directly against the data plane. The control plane search is the access control gate — clients only receive IDs they are authorized to access. The data plane never evaluates metadata; it serves measurements for the requested IDs, relying on the sorted Parquet structure and DuckLake column statistics for efficient lookup.

### Raw Data Fingerprint

A SHA-256 fingerprint of uploaded raw data is recorded per `prep_sample_idx` at upload time in the control plane. Its purpose is **upload-time duplicate detection only** — it is not the processing deduplication key:

- Warns users when uploaded data appears identical to an existing `prep_sample_idx`
- Surfaces accidental duplicate uploads before compute is wasted
- Provides the basis for storage deduplication (one physical file, multiple logical references) as a future optimisation

### Processing Deduplication and Disallow-Without-Delete

The control plane gates all job submission on the current state of each `(prep_sample_idx, processing_idx)` pair. Before submitting any work:

- **COMPLETED**: disallow — require explicit DELETE before resubmission
- **PENDING, QUEUED, or PROCESSING**: disallow — work is already in flight; submitting again would produce duplicate compute for the same result
- **FAILED** or absent: allow submission

This check applies at both the work ticket level (is there an active ticket for this prep + processing combination?) and the individual sample level (does any `prep_sample_idx` in the request already have a result in a non-terminal state?), preventing both whole-prep and partial duplicate submissions.

The data plane asserts identifier integrity programmatically since DuckLake does not support explicit constraints (no unique constraints, no foreign keys at the DuckLake level):

- **At registration**: before `ducklake_add_data_files`, verifies the Parquet file's `processed_prep_sample_idx` values are a subset of the expected set provided by the control plane, and that no value from the file already exists in the catalog for this `(prep_idx, processing_idx)` combination.
- **At service startup**: scans the DuckLake catalog to verify no `processed_prep_sample_idx` appears in multiple active files for the same `(prep_idx, processing_idx)`. Violations are logged as critical errors and the affected combinations are blocked from serving until reconciled.

## Client Interfaces (Unresolved)

Client interfaces are the user-facing layer through which researchers and systems interact with Qiita. All interfaces authenticate via AuthRocket (OAuth2/JWT) and connect through the nginx gateway.

**Candidate interfaces** (to be discussed):
- **CLI** — command-line tool for scripted/automated workflows. Would speak both REST (control plane) and Arrow Flight (data plane) for data upload/download.
- **Web application** — browser-based UI for study management, search, and monitoring. REST-only (control plane). Bulk data transfer would be delegated to CLI or SDK.
- **Python/R SDK** — programmatic library for use in analysis scripts and pipelines. Would wrap both REST and Arrow Flight APIs, providing native DataFrame integration (pandas, polars, R data.frame).
- **Notebook integration** — Jupyter/RStudio integration, likely built on top of the SDK.

## Arrow Flight Operations (no custom .proto needed)

- **DoGet**: select by key (table + integer identifiers encoded in signed Flight ticket)
- **DoPut**: upload data (stream RecordBatches to shared filesystem via FlightDescriptor, authorized by signed action token)
- **DoAction**: register file (`ducklake_add_data_files`), delete by key, insert-from-processing-method (authorized by signed action token)

## Auth & Data Access Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant AR as AuthRocket
    participant NX as nginx
    participant CP as Control Plane<br/>(FastAPI)
    participant DP as Data Plane<br/>(Arrow Flight)
    participant PG_APP as Postgres<br/>(App DB)
    participant PG_DL as Postgres<br/>(DuckLake Catalog)
    participant DL as DuckLake<br/>(Parquet on disk)

    Note over C,AR: 1. Authentication
    C->>AR: OAuth2 login
    AR-->>C: JWT

    Note over C,CP: 2. Authorization & ticket issuance
    C->>NX: REST request + JWT<br/>(e.g., "read table foo where bar_idx=5")
    NX->>CP: route REST
    CP->>PG_APP: check user roles/permissions
    PG_APP-->>CP: authorized
    CP-->>NX: signed Flight ticket<br/>(HMAC-SHA256, short-lived, scoped)
    NX-->>C: signed Flight ticket

    Note over C,DP: 3. Data access
    C->>NX: DoGet(signed_ticket) + JWT in gRPC metadata
    NX->>DP: route gRPC (HTTP/2)
    DP->>DP: verify JWT (cached JWKS)
    DP->>DP: verify ticket signature (HMAC)
    DP->>PG_DL: query DuckLake catalog
    PG_DL-->>DP: file references
    DP->>DL: read Parquet files
    DL-->>DP: data
    DP-->>NX: stream RecordBatches
    NX-->>C: stream RecordBatches

    Note over DP,CP: 4. Work ticket callback
    DP->>NX: REST callback + service credential
    NX->>CP: route REST
    CP->>PG_APP: update work ticket status
```

**Text flow:**

1. **Authentication:** Client authenticates with AuthRocket via OAuth2, receives a JWT.
2. **Authorization & ticket issuance:** Client sends REST request to control plane (through nginx) with JWT. Control plane checks user roles/permissions in the app database. If authorized, control plane returns a signed Flight ticket (HMAC-SHA256, short-lived, scoped to the specific operation and parameters).
3. **Data access:** Client sends the signed Flight ticket to the data plane (through nginx, gRPC/HTTP/2) with JWT in gRPC metadata. Data plane verifies the JWT locally (cached AuthRocket JWKS public keys) and verifies the ticket signature (shared HMAC secret). Data plane queries the DuckLake catalog in Postgres, reads Parquet files from local/shared disk, and streams Arrow RecordBatches back to the client.
4. **Work ticket callback:** On completion or failure of a processing job, data plane calls a REST endpoint on the control plane to update work ticket status. Data plane authenticates this callback with a service credential.

## Data Upload & Processing Workflow

```mermaid
sequenceDiagram
    participant C as Client
    participant NX as nginx
    participant CP as Control Plane<br/>(FastAPI)
    participant CO as Compute<br/>Orchestrator
    participant SR as slurmrestd
    participant SL as SLURM Job<br/>(Container)
    participant DP as Data Plane<br/>(Arrow Flight)
    participant PG_APP as Postgres<br/>(App DB)
    participant FS as Shared<br/>Filesystem

    Note over C,CP: 1. Upload request
    C->>NX: REST: "upload amplicon data for study 42, prep 7" + JWT
    NX->>CP: route REST
    CP->>PG_APP: validate access, create work ticket (PENDING)
    CP-->>C: signed Flight ticket for DoPut

    Note over C,DP: 2. Data upload
    C->>NX: DoPut(signed_ticket) + JWT + FASTQ stream
    NX->>DP: route gRPC
    DP->>DP: verify JWT + ticket signature
    DP->>FS: write FASTQ to /data/staging/study42/prep7/
    DP-->>C: upload confirmed

    Note over DP,CP: 3. Upload complete callback
    DP->>CP: REST callback: upload complete, path=/data/staging/study42/prep7/
    CP->>PG_APP: update work ticket (UPLOADED)

    Note over CP,CO: 4. Compute submission
    CP->>CO: REST: submit processing for work ticket X
    CO->>SR: POST /slurm/{slurmrestd_api_ver}/job/submit<br/>(container: qiita-workflow-amplicon:v1.2.0,<br/>input/output paths, stdout/stderr log paths)
    SR-->>CO: job_id=98765
    CO->>CP: REST callback: SLURM job queued, job_id=98765
    CP->>PG_APP: update work ticket (QUEUED, slurm_job_id=98765)

    Note over CO,SR: 5. Job monitoring (orchestrator polls)
    CO->>SR: GET /slurm/{slurmrestd_api_ver}/job/98765
    SR-->>CO: state=RUNNING
    CO->>CP: REST callback: job running
    CP->>PG_APP: update work ticket (PROCESSING)

    Note over SL,FS: 6. SLURM execution
    SL->>FS: read /data/staging/study42/prep7/
    SL->>SL: run amplicon processing workflow
    SL->>FS: write /data/results/study42/prep7/output.parquet
    SL->>FS: stdout/stderr → /data/logs/study42/prep7/job-98765.{out,err}

    Note over CO,CP: 7. Completion detection & file registration
    CO->>SR: GET /slurm/{slurmrestd_api_ver}/job/98765
    SR-->>CO: state=COMPLETED, exit_code=0
    CO->>FS: verify /data/results/study42/prep7/output.parquet exists
    CO->>FS: collect log paths
    CO->>CP: REST (as compute user): job 98765 succeeded,<br/>output=/data/results/.../output.parquet,<br/>logs=/data/logs/.../job-98765.{out,err}
    CP->>PG_APP: validate work ticket state
    CP->>DP: register file into DuckLake
    DP->>DP: CALL ducklake_add_data_files(catalog, T, path)<br/>(metadata only — no I/O, schema validated)
    DP-->>CP: file registered
    CP->>PG_APP: update work ticket (COMPLETED),<br/>record provenance + log paths

    Note over CO,CP: 7a. Failure handling (alternative)
    CO->>SR: GET /slurm/{slurmrestd_api_ver}/job/98765
    SR-->>CO: state=FAILED, exit_code=1
    CO->>FS: collect log paths
    CO->>CP: REST: job 98765 failed, exit_code=1,<br/>logs=/data/logs/.../job-98765.{out,err},<br/>failure_type=job_error
    CP->>PG_APP: increment retry_count,<br/>requeue if retries < max_retries,<br/>else mark FAILED
```

**Text flow:**

1. **Upload request:** Client sends REST request to control plane with JWT. Control plane validates access, creates a work ticket (PENDING), and returns a signed Flight ticket authorizing a DoPut upload.
2. **Data upload:** Client streams raw data (e.g., FASTQ) to the data plane via Arrow Flight DoPut through nginx. Data plane verifies JWT and ticket signature, writes data to the shared filesystem at a structured staging path.
3. **Upload complete callback:** Data plane calls back to control plane with the staging path. Control plane updates the work ticket to UPLOADED.
4. **Compute submission:** Control plane requests the compute orchestrator submit a SLURM job via slurmrestd. The job specifies a container image (e.g., `qiita-workflow-amplicon:v1.2.0`), input/output paths on the shared filesystem, and stdout/stderr log paths. SLURM jobs have no knowledge of the control plane — they are truly dumb (read input, process, write output, exit). Compute orchestrator returns the SLURM job ID. Control plane updates the work ticket to QUEUED.
5. **Job monitoring:** Compute orchestrator polls slurmrestd for job status. When the job transitions to RUNNING, it notifies the control plane. Control plane updates the work ticket to PROCESSING.
6. **SLURM execution:** The containerized workflow runs on the SLURM cluster, reading input from the staging path on the shared filesystem and writing Parquet results to the results path. Stdout/stderr are captured to log files on the shared filesystem.
7. **Completion detection & file registration:** Compute orchestrator detects job completion via slurmrestd polling. It verifies the output file exists on the shared filesystem, collects log file paths, and calls the control plane. Control plane validates the work ticket state, then instructs the data plane to register the Parquet file into DuckLake via `ducklake_add_data_files` (metadata-only operation — no I/O, only schema validation). On success, the control plane updates the work ticket to COMPLETED and records provenance (who, what, when, which workflow version, SLURM job ID, log paths).
7a. **Failure handling:** If the SLURM job fails, the compute orchestrator detects the failure, collects log paths, and reports to the control plane with the failure type and exit code. The control plane increments the retry count. If retries remain, it requeues the job (back to QUEUED). If max retries are exhausted, it marks the work ticket as FAILED with the failure reason, stage, and log paths for diagnosis.

## Work Ticket Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PENDING: work ticket created
    PENDING --> UPLOADED: data upload confirmed
    PENDING --> FAILED: upload error (retries exhausted)
    UPLOADED --> QUEUED: SLURM job submitted
    UPLOADED --> FAILED: submission error (retries exhausted)
    QUEUED --> PROCESSING: SLURM job running
    QUEUED --> FAILED: queue error (retries exhausted)
    PROCESSING --> COMPLETED: results registered in DuckLake
    PROCESSING --> QUEUED: retriable failure (auto-retry)
    PROCESSING --> FAILED: permanent failure or retries exhausted
    FAILED --> QUEUED: manual restart
    COMPLETED --> [*]
    FAILED --> [*]
```

States:
- **PENDING** — work ticket created, awaiting data upload
- **UPLOADED** — raw data on shared filesystem, awaiting compute submission
- **QUEUED** — SLURM job submitted, waiting for cluster resources (slurm_job_id recorded)
- **PROCESSING** — SLURM job actively running on cluster
- **COMPLETED** — results registered in DuckLake, provenance recorded
- **FAILED** — failure at any stage, retries exhausted or permanent failure

Work ticket fields:
- `id` — unique identifier
- `study_id`, `preparation_id` — references to control plane entities
- `state` — current lifecycle state
- `current_step` — index of the step currently executing or last attempted (0-based)
- `total_steps` — total number of steps in the workflow
- `failed_stage` — which stage failed (if FAILED): `upload`, `submission`, `processing_step_{n}`, `registration`
- `failure_reason` — human-readable failure description
- `failure_type` — `retriable` or `permanent`
- `retry_count` — number of retries attempted
- `max_retries` — configurable maximum (default: 3)
- `slurm_job_id` — SLURM job identifier for the current step (updated each step; for map steps, the most recently failed or active job ID)
- `failed_samples` — list of `prep_sample_idx` values that did not survive the map phase (retries exhausted); recorded before the reduce step runs
- `staging_path` — path to uploaded raw data
- `result_path` — path to processed output
- `log_stdout_path` — path to SLURM job stdout
- `log_stderr_path` — path to SLURM job stderr
- `workflow_name`, `workflow_version` — which workflow was executed
- `created_at`, `updated_at`, `completed_at` — timestamps
- `created_by` — user who initiated the work ticket
- `provenance` — JSON blob with full audit trail (set at COMPLETED)

Retriable failure types:
- SLURM node failure / preemption
- OOM kill (may succeed with different resource allocation)
- Transient filesystem errors
- slurmrestd communication failures

Permanent failure types:
- Invalid input data (corrupt FASTQ, wrong format)
- Schema mismatch at DuckLake registration
- Workflow container image not found
- Workflow exit with known permanent error code
- Container contract violation (manifest missing or identifier set mismatch after exit code 0)

Manual restart:
- An authorized user can restart a FAILED work ticket via the control plane REST API
- This resets `retry_count` to 0 and transitions the ticket to QUEUED
- The original failure information is preserved in the provenance log

## Compute Orchestrator

Separate Python service responsible for the full compute job lifecycle.

**Lifecycle ownership:** The compute orchestrator owns everything between "submit job" and "report result to control plane." SLURM jobs have no knowledge of the control plane — they are truly dumb (read input, process, write output, exit). As their final act before exiting, jobs must `chmod 440` all output files and write a manifest (see Container Contract below). The data plane enforces the permission check as a pre-registration gate.

**Multi-step workflows:** Workflows consist of one or more sequential steps, each with independent resource requirements and a step type of `map` or `reduce`. Steps are submitted as separate SLURM jobs so each is sized for its actual resource needs.

**Step types:**

- **`map`** — sample-independent. The orchestrator fans out one SLURM job per `prep_sample_idx` in parallel. Each job receives a `params.json` containing the full identifier set for that sample plus processing parameters, and produces a single-sample Parquet to its own output directory. Map jobs are retried independently — a failed sample is retried without reprocessing survivors. `failed_samples` on the work ticket accumulates any `prep_sample_idx` values that exhaust retries.
- **`reduce`** — prep-level. Executes once all map jobs for the preceding step have completed. Receives all surviving map output directories as its input. Must produce a single Parquet sorted by `(study_idx, prep_idx, sample_idx, prep_sample_idx, processing_idx, processed_prep_sample_idx)`. Two reducer implementations:
  - **`platform/sort-merge`**: generic platform-provided container — DuckDB reads all input Parquet files, sorts by the standard identifier columns, writes output. No workflow-specific code required for pure aggregation.
  - **workflow-specific**: custom container for cross-sample computation (normalisation, diversity metrics, etc.). Must still output sorted by the standard identifier columns as part of the container contract.

The orchestrator drives execution:

1. For each `map` step: write per-sample `params.json`, fan out N SLURM jobs (one per `prep_sample_idx`), poll all, retry failed samples independently, accumulate `failed_samples`
2. For each `reduce` step: write `params.json` containing the expected `processed_prep_sample_idx` set for surviving samples, submit one SLURM job with all map output directories as input, poll to completion
3. Verify three-gate output for every job (map and reduce)
4. Advance `current_step` on the work ticket and continue
5. After the final step, call back to the control plane to trigger data plane registration

Intermediate outputs: `/data/staging/{study_idx}/{prep_idx}/{ticket_id}/step_{n}/{prep_sample_idx}/` (map), `/data/staging/{study_idx}/{prep_idx}/{ticket_id}/step_{n}/` (reduce). Final results: `/data/results/{study_idx}/{prep_idx}/{ticket_id}/`.

Failure records which step failed (`failed_stage=processing_step_{n}`). Manual restart resets to step 0.

**Container contract:** Every workflow container must honour this interface regardless of what it does internally. Violations are rejected by the orchestrator's output verification gates and treated as permanent failures.

Inputs (orchestrator provides before submission):
- `QIITA_INPUT_PATH` env var — directory to read input from
- `QIITA_OUTPUT_PATH` env var — directory to write output to
- `$QIITA_INPUT_PATH/params.json` — written by the orchestrator; absent if the step has no parameters

For `map` steps, `params.json` contains the sample's full identifier set and processing parameters:
```json
{
  "study_idx": 1, "prep_idx": 3, "sample_idx": 7,
  "prep_sample_idx": 42, "processing_idx": 10, "processed_prep_sample_idx": 99,
  "parameters": { }
}
```

For `reduce` steps, `params.json` contains the prep-level identifiers, the surviving sample set, and any parameters:
```json
{
  "study_idx": 1, "prep_idx": 3, "processing_idx": 10,
  "surviving_samples": [
    {"prep_sample_idx": 42, "processed_prep_sample_idx": 99}
  ],
  "parameters": { }
}
```

Outputs (container must produce):
- All output files written to `$QIITA_OUTPUT_PATH`
- `$QIITA_OUTPUT_PATH/manifest.json` written as the final act before chmod, listing every output file with its size in bytes:
  ```json
  {"files": [{"path": "output.parquet", "size_bytes": 12345678}]}
  ```
- All files in `$QIITA_OUTPUT_PATH` set to `chmod 440` (including the manifest)
- Exit code 0 on success, non-zero on any failure

The container must not read from anywhere other than `$QIITA_INPUT_PATH`, must not write to anywhere other than `$QIITA_OUTPUT_PATH`, and must have no knowledge of Qiita, the control plane, or any service credentials.

Orchestrator verification gates (all three must pass before a step is accepted):
1. SLURM exit code 0
2. `$QIITA_OUTPUT_PATH/manifest.json` exists
3. Every file listed in the manifest exists and matches the declared `size_bytes`

Gates 2 or 3 failing after exit code 0 is a permanent failure — the container violated the contract.

**Primary backend:** SLURM via slurmrestd REST API (JWT auth).
- Submits jobs as JSON (no `#SBATCH` directives — slurmrestd ignores them)
- Environment variables explicitly specified in submission payload
- Polls job status via `GET /slurm/{slurmrestd_api_ver}/job/{job_id}` (version is a config parameter; target SLURM ≥ 25.x.x)
- Runs output verification gates on completion
- Reports results back to control plane via REST callback after all steps pass
- Shared filesystem assumed for all data I/O

**Job logging:** SLURM captures stdout/stderr to files on the shared filesystem at `/data/logs/{study_id}/{prep_id}/{ticket_id}/step_{n}-{slurm_job_id}.{out,err}`. All step log paths are recorded on the work ticket.

**Workflow containerization:** Apptainer/Singularity for HPC compatibility. (Apptainer is the Linux Foundation continuation of Singularity; the `singularity` command is typically aliased to `apptainer`.)
- Container images per workflow step, versioned (e.g., `qiita-workflow-amplicon:v1.2.0`)
- Workflow definitions in monorepo as config: ordered steps, each with container image, entrypoint, and resource requirements
- No root required (runs unprivileged)
- SLURM submits via `srun apptainer exec` or native integration

**Future:** Clean `ComputeBackend` interface allows adding alternative backends (cloud, Kubernetes) without changing the control plane.

## Health Checks

Each service exposes a health check endpoint for monitoring and deployment verification.

- **qiita-control-plane:** `GET /health` — checks Postgres connectivity, returns service version
- **qiita-data-plane:** gRPC health check protocol (`grpc.health.v1.Health/Check`) — checks DuckLake catalog connectivity
- **qiita-compute-orchestrator:** `GET /health` — checks slurmrestd reachability, returns service version
- **nginx:** proxies health checks; can be used for readiness gating during deploys

## Service Accounts

- **`compute`** — internal service account for compute orchestrator. Authenticates to control plane with pre-shared API key. Narrow permissions: update work tickets, request file registration. Audit trail links actions to specific work tickets and SLURM job IDs.
- **`data-plane`** — internal service account for data plane callbacks to control plane. Pre-shared API key. Permissions: update work ticket status on upload completion/failure.

## Work Ticket Queue

Postgres-based (`SELECT ... FOR UPDATE SKIP LOCKED`). Work tickets created by qiita-control-plane. Work ticket state transitions driven by callbacks from data plane and compute orchestrator.

## Database Topology

Single hardened Postgres instance, two logical databases:
- **App DB** — control plane tables: users, roles, studies, samples, preparations, work tickets, provenance
- **DuckLake Catalog DB** — DuckLake metadata tables (snapshots, data files, schemas, etc.)

## Data Storage

Shared filesystem accessible by all components:
- `/data/staging/` — raw uploaded data (FASTQ, etc.). Cleanup policy TBD.
- `/data/results/` — processed output (Parquet). Ownership transfers to DuckLake after registration; compaction may delete/rewrite files.
- `/data/logs/` — SLURM job stdout/stderr logs. Retained for diagnosis.

## Ticket Signing

HMAC-SHA256 with a shared secret configured in both control plane and data plane services.

## Deployment

On-premise Linux, systemd services. Local dev on macOS.

The `make deploy` target builds all components and prints the required admin commands for systemd/nginx installation. An admin executes the privileged commands manually.

The data plane is deployed as multiple systemd instances (`qiita-data-plane@1`, `qiita-data-plane@2`, ... `qiita-data-plane@N`) on separate ports, with nginx upstream configured to load-balance gRPC traffic across them. Instance count is tunable without code changes — only the nginx upstream block and the number of systemd units need updating.

## Monorepo Structure

```
qiita/
├── Makefile                        # unified entry point: build, test, lint, deploy, migrate
├── .github/
│   └── workflows/
│       ├── ci.yml                  # runs: make lint && make test && make test-integration
│       └── deploy.yml              # runs: make deploy (prints admin commands)
├── qiita-common/
│   ├── pyproject.toml              # shared Pydantic models, config, client utilities
│   └── src/
│       └── qiita_common/
│           ├── __init__.py
│           ├── models.py           # work ticket states, API schemas, shared types
│           ├── config.py           # shared config patterns (env var loading, etc.)
│           └── client.py           # REST client utilities for service-to-service calls
├── qiita-control-plane/
│   ├── pyproject.toml              # uv-managed, depends on qiita-common
│   ├── uv.lock
│   ├── db/
│   │   └── migrations/             # dbmate SQL migration files
│   ├── src/
│   │   └── qiita_control_plane/
│   │       ├── __init__.py
│   │       ├── main.py             # FastAPI app entry point + /health endpoint
│   │       ├── config.py           # settings (DB URL, HMAC secret, AuthRocket JWKS URL)
│   │       ├── auth/               # JWT verification, HMAC ticket signing, AuthRocket integration
│   │       ├── models/             # asyncpg models (studies, samples, preparations, users, roles, work tickets, provenance)
│   │       ├── routes/
│   │       │   ├── studies.py
│   │       │   ├── samples.py
│   │       │   ├── preparations.py
│   │       │   ├── tickets.py      # work ticket CRUD + Flight ticket issuance + manual restart
│   │       │   └── callbacks.py    # data plane + compute orchestrator callback endpoints
│   │       └── db.py               # asyncpg connection pool setup
│   └── tests/
│       ├── conftest.py
│       └── ...
├── qiita-data-plane/
│   ├── Cargo.toml                  # deps: arrow-flight, tonic, duckdb, hmac, sha2, jsonwebtoken
│   └── src/
│       ├── main.rs                 # tonic server entry, Flight service + gRPC health check registration
│       ├── config.rs               # settings (DuckLake catalog DB URL, HMAC secret, JWKS URL, control plane callback URL)
│       ├── flight_service.rs       # impl FlightService trait (do_get, do_put, do_action)
│       ├── auth.rs                 # JWT verification (cached JWKS), HMAC ticket verification
│       ├── ducklake.rs             # DuckDB/DuckLake connection management, ducklake_add_data_files
│       └── callback.rs             # REST client to call back control plane
├── qiita-compute-orchestrator/
│   ├── pyproject.toml              # uv-managed, depends on qiita-common
│   ├── uv.lock
│   ├── src/
│   │   └── qiita_compute_orchestrator/
│   │       ├── __init__.py
│   │       ├── main.py             # service entry point + /health endpoint
│   │       ├── config.py           # settings (slurmrestd URL, slurmrestd JWT, control plane URL, poll interval)
│   │       ├── backend.py          # ComputeBackend interface
│   │       ├── slurm.py            # SLURM implementation (slurmrestd REST client, job lifecycle)
│   │       └── workflows/          # workflow definitions (container image, resources, entrypoint)
│   └── tests/
│       ├── conftest.py
│       └── ...
├── tests/
│   └── integration/
│       ├── conftest.py             # test fixtures: Postgres, services, test data
│       ├── docker-compose.yml      # Postgres for integration tests
│       ├── test_upload_workflow.py  # end-to-end upload → process → register
│       └── test_auth_flow.py       # end-to-end auth → ticket → data access
├── workflows/
│   ├── amplicon/
│   │   ├── Apptainer.def           # container definition (single image for all steps)
│   │   ├── workflow.yaml           # ordered steps: name, type (map|reduce), entrypoint, resources
│   │   └── scripts/                # per-step entrypoints and helpers
│   ├── metagenomic/
│   │   └── ...
│   └── ...
├── deploy/
│   ├── systemd/
│   │   ├── qiita-control-plane.service
│   │   ├── qiita-data-plane.service
│   │   └── qiita-compute-orchestrator.service
│   └── nginx/
│       └── qiita.conf              # REST and gRPC routing, TLS termination, HTTP/2
└── .gitignore
```

## Build System (Makefile)

```makefile
.PHONY: build test test-integration lint deploy migrate clean verify-health

# Build all components
build: build-common build-control-plane build-data-plane build-compute-orchestrator build-workflows

build-common:
	cd qiita-common && uv sync

build-control-plane:
	cd qiita-control-plane && uv sync

build-data-plane:
	cd qiita-data-plane && cargo build --release

build-compute-orchestrator:
	cd qiita-compute-orchestrator && uv sync

build-workflows:
	@for dir in workflows/*/; do \
		if [ -f "$$dir/Apptainer.def" ]; then \
			apptainer build "$$dir/$$(basename $$dir).sif" "$$dir/Apptainer.def"; \
		fi \
	done

# Run unit tests
test: test-common test-control-plane test-data-plane test-compute-orchestrator

test-common:
	cd qiita-common && uv run pytest

test-control-plane:
	cd qiita-control-plane && uv run pytest

test-data-plane:
	cd qiita-data-plane && cargo test

test-compute-orchestrator:
	cd qiita-compute-orchestrator && uv run pytest

# Run integration tests (requires Docker for Postgres)
test-integration:
	cd tests/integration && docker compose up -d --wait
	cd tests/integration && uv run pytest
	cd tests/integration && docker compose down

# Lint all components
lint: lint-common lint-control-plane lint-data-plane lint-compute-orchestrator

lint-common:
	cd qiita-common && uv run ruff check . && uv run ruff format --check .

lint-control-plane:
	cd qiita-control-plane && uv run ruff check . && uv run ruff format --check .

lint-data-plane:
	cd qiita-data-plane && cargo clippy -- -D warnings && cargo fmt --check

lint-compute-orchestrator:
	cd qiita-compute-orchestrator && uv run ruff check . && uv run ruff format --check .

# Run database migrations
migrate:
	cd qiita-control-plane && dbmate up

# Build and print deploy instructions (no sudo)
deploy: build
	@echo "=== Build complete. Run the following commands as admin: ==="
	@echo ""
	@echo "  sudo cp deploy/systemd/qiita-control-plane.service /etc/systemd/system/"
	@echo "  sudo cp deploy/systemd/qiita-data-plane.service /etc/systemd/system/"
	@echo "  sudo cp deploy/systemd/qiita-compute-orchestrator.service /etc/systemd/system/"
	@echo "  sudo cp deploy/nginx/qiita.conf /etc/nginx/conf.d/"
	@echo "  sudo systemctl daemon-reload"
	@echo "  sudo systemctl restart qiita-control-plane"
	@echo "  sudo systemctl restart qiita-data-plane"
	@echo "  sudo systemctl restart qiita-compute-orchestrator"
	@echo "  sudo systemctl reload nginx"
	@echo ""
	@echo "Then verify: make verify-health"

# Verify all services are healthy after deploy
verify-health:
	@echo "Checking control plane..."
	@curl -sf http://localhost:8080/health || (echo "FAIL: control plane" && exit 1)
	@echo " OK"
	@echo "Checking compute orchestrator..."
	@curl -sf http://localhost:8081/health || (echo "FAIL: compute orchestrator" && exit 1)
	@echo " OK"
	@echo "Checking data plane..."
	@grpcurl -plaintext localhost:50051 grpc.health.v1.Health/Check || (echo "FAIL: data plane" && exit 1)
	@echo " OK"
	@echo "All services healthy."

clean:
	cd qiita-common && rm -rf .venv __pycache__
	cd qiita-control-plane && rm -rf .venv __pycache__
	cd qiita-data-plane && cargo clean
	cd qiita-compute-orchestrator && rm -rf .venv __pycache__
```

## CI (GitHub Actions)

```yaml
# .github/workflows/ci.yml
name: CI
on: [push, pull_request]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make lint

  test-unit:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make test

  test-integration:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: test
        ports:
          - 5432:5432
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: astral-sh/setup-uv@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: make test-integration
```

## Wiring Notes

- **Shared HMAC secret:** Both control plane and data plane read from environment variable or config file. Rotated by deploying a new secret to both services and restarting.
- **JWKS caching:** Both services fetch AuthRocket's `/.well-known/jwks.json` on startup and refresh periodically (e.g., every 5 minutes). No per-request calls to AuthRocket.
- **nginx gRPC config:** Requires `grpc_pass` directive (not `proxy_pass`), `http2` on the listener, TLS termination. REST and gRPC routes split by path prefix or content-type.
- **slurmrestd:** Compute orchestrator authenticates to slurmrestd via SLURM JWT. All job parameters specified in JSON body (not `#SBATCH` directives). Environment variables must be explicitly listed.
- **DuckLake file registration:** `ducklake_add_data_files` registers a Parquet file by path — no data copying. Ownership transfers to DuckLake (compaction may later delete/rewrite). Schema and type validation performed at registration time. Registration failure (schema mismatch, corrupt Parquet) is an explicit FAILED state with `failed_stage=registration` and `failure_type=permanent`.
- **Service-to-service auth:** Data plane and compute orchestrator authenticate to control plane via pre-shared API keys for their respective service accounts (`data-plane`, `compute`).
- **Shared filesystem paths:** All components (control plane, data plane, SLURM jobs) access the same filesystem. Staging path: `/data/staging/{study_id}/{prep_id}/`. Results path: `/data/results/{study_id}/{prep_id}/`. Logs path: `/data/logs/{study_id}/{prep_id}/`.
- **qiita-common dependency:** Both Python services depend on `qiita-common` as a path dependency in their `pyproject.toml` (e.g., `qiita-common = {path = "../qiita-common"}`). Shared Pydantic models ensure API contract consistency.
- **Data plane Unix user and file protection:** The data plane runs as a dedicated `qiita-data-plane` system user (no login shell). SLURM jobs run as a separate `qiita-worker` user and write result files to `/data/results/`. Before exiting, each job sets `chmod 440` on its output files — owner and group read-only, no write, no world access. The data plane checks this permission as a pre-registration gate before calling `ducklake_add_data_files`; a file that is not `440` is rejected as a permanent registration failure. This ensures that once DuckLake takes ownership of a file, no compute job (or other process running as `qiita-worker`) can overwrite or corrupt it. The data plane user must be in the same group as `qiita-worker` (or the results directory must be group-readable) so the service can read files it does not own. Staging files (`/data/staging/`) follow the same pattern: `qiita-worker` writes them, and they are read-only to all other users once the upload is confirmed.
- **Data plane horizontal scaling:** The data plane is the read/write path for all DuckLake data. At scale, many concurrent SLURM jobs may issue large DoGet reads simultaneously, making a single data plane process a throughput bottleneck. The data plane scales horizontally because each instance is stateless with respect to request handling — it holds only a DuckDB+DuckLake connection to the shared Postgres catalog and reads Parquet files from the shared filesystem. DuckLake's concurrent read model is safe for this: multiple DuckDB instances connecting to the same Postgres catalog never block each other (readers use snapshot isolation with no row-level locking; conflicts only arise on concurrent writes, resolved via optimistic concurrency at commit time). Workers cannot bypass the data plane and read Parquet files directly for several reasons: (1) deletions are recorded as separate delete files in the DuckLake catalog — raw Parquet reads return logically deleted rows; (2) small inserts below the data inlining threshold are stored entirely within the Postgres catalog (`ducklake_inlined_data_tables`), with no Parquet file written at all; (3) snapshot visibility requires a catalog query to determine which files are live for the current consistent state; (4) compaction rewrites and deletes Parquet files under active management, making cached paths unreliable. The data plane remains the correct and only correct read path; the solution to the bottleneck is running more of them.
