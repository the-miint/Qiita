# Qiita Architecture

Qiita is a scalable multi-omic study management, processing, and analysis platform for microbiome data (amplicon, metagenomic, metatranscriptomic, metabolomic, proteomic). Schema informed by existing Qiita data model (carried forward surgically by human guidance) and BioSample (scope TBD).

**Scale:** Millions of samples, 100s of TB of data.

## Contents

The reference is split by topic. The bullets under each file are its top-level
headings, so a grep here lands on the file that carries the detail.

### [System Overview](architecture/overview.md)

The service diagram, and what each of the four components is.

- [System Architecture](architecture/overview.md#system-architecture)
- [Components](architecture/overview.md#components)

### [Data Model](architecture/data-model.md)

Identifier hierarchy, Parquet identifier columns, metadata, processing dedup.

- [Identifier Hierarchy](architecture/data-model.md#identifier-hierarchy)
- [Legacy import reserved range — study and prep_sample](architecture/data-model.md#legacy-import-reserved-range--study-and-prep_sample)
- [Processing Methods](architecture/data-model.md#processing-methods)
- [Identifier Columns in Parquet](architecture/data-model.md#identifier-columns-in-parquet)
- [Biosample and Prep Sample Metadata](architecture/data-model.md#biosample-and-prep-sample-metadata)
- [Raw Data Fingerprint](architecture/data-model.md#raw-data-fingerprint)
- [Processing Deduplication and Disallow-Without-Delete](architecture/data-model.md#processing-deduplication-and-disallow-without-delete)

### [Reference Database Design](architecture/reference-data.md)

Three-level identity model, taxonomy, phylogeny, exclusion, aligner indices, host filtering.

- [Three-Level Identity Model](architecture/reference-data.md#three-level-identity-model)
- [Control Plane vs. Data Plane Split](architecture/reference-data.md#control-plane-vs-data-plane-split)
- [Taxonomy as a Reference](architecture/reference-data.md#taxonomy-as-a-reference)
- [Annotations](architecture/reference-data.md#annotations)
- [Phylogeny and Placements](architecture/reference-data.md#phylogeny-and-placements)
- [Reference exclusion (curated blocklist)](architecture/reference-data.md#reference-exclusion-curated-blocklist)
- [Aligner Index Storage](architecture/reference-data.md#aligner-index-storage)
- [Shard planner](architecture/reference-data.md#shard-planner)
- [Reference sequence streaming (build jobs)](architecture/reference-data.md#reference-sequence-streaming-build-jobs)
- [Sharded-index fan-out](architecture/reference-data.md#sharded-index-fan-out)
- [Sharded alignment + foundation + consumer](architecture/reference-data.md#sharded-alignment--foundation--consumer)
- [Host references](architecture/reference-data.md#host-references)
- [Host-filter resolution](architecture/reference-data.md#host-filter-resolution)
- [Bulk Reference Ingestion](architecture/reference-data.md#bulk-reference-ingestion)
- [Scale](architecture/reference-data.md#scale)
- [Example Query Patterns](architecture/reference-data.md#example-query-patterns)

### [Arrow Flight Surface](architecture/flight.md)

DoGet/DoPut/DoAction, compression, column projection, which mints are REST rather than Flight.

- [Client Interfaces (Unresolved)](architecture/flight.md#client-interfaces-unresolved)
- [Arrow Flight Operations (no custom .proto needed)](architecture/flight.md#arrow-flight-operations-no-custom-proto-needed)

### [Processing and Work Tickets](architecture/processing.md)

Auth and data-access flow, upload and processing, work ticket lifecycle, the orchestrator, health.

- [Auth & Data Access Flow](architecture/processing.md#auth--data-access-flow)
- [Data Upload & Processing Workflow](architecture/processing.md#data-upload--processing-workflow)
- [Work Ticket Lifecycle](architecture/processing.md#work-ticket-lifecycle)
- [Compute Orchestrator](architecture/processing.md#compute-orchestrator)
- [Health Checks](architecture/processing.md#health-checks)
- [Work Ticket Queue](architecture/processing.md#work-ticket-queue)

### [Storage and Topology](architecture/storage.md)

Database topology, the /data and /scratch layout, ticket signing.

- [Database Topology](architecture/storage.md#database-topology)
- [Data Storage](architecture/storage.md#data-storage)
- [Ticket Signing](architecture/storage.md#ticket-signing)

### [Build, Layout and CI](architecture/build-and-deploy.md)

Deployment shape, monorepo tree, Makefile targets, GitHub Actions.

- [Deployment](architecture/build-and-deploy.md#deployment)
- [Monorepo Structure](architecture/build-and-deploy.md#monorepo-structure)
- [Build System (Makefile)](architecture/build-and-deploy.md#build-system-makefile)
- [CI (GitHub Actions)](architecture/build-and-deploy.md#ci-github-actions)

### [Wiring and Cross-cutting Structure](architecture/cross-cutting.md)

Wiring notes, component map and ports, identifier ownership, data-plane design, the runner.

- [Wiring Notes](architecture/cross-cutting.md#wiring-notes)
- [Cross-cutting structure](architecture/cross-cutting.md#cross-cutting-structure)
