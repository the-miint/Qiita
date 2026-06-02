# Reference Data Staging Convention

Source data for reference database ingestion is staged on the shared filesystem before the ingestion pipeline runs. This document defines the directory layout and manifest format.

## Directory Layout

```
{PATH_SCRATCH}/references/staging/{name}/{version}/
├── seqs.fna.gz                # Sequences (FASTA, gzipped)
├── taxonomy.tsv.gz            # Taxonomy assignments (TSV, gzipped)
├── backbone.nwk               # Backbone phylogeny (Newick)
├── placements.jplace          # Phylogenetic placements (jplace)
├── genome_mapping.tsv         # Feature-to-genome associations (TSV)
└── manifest.json              # Declares what's present + checksums
```

In development, `{PATH_SCRATCH}` defaults to `$TMPDIR/qiita` (or `/tmp/qiita` if `$TMPDIR` is unset). In production, it is the shared scratch mount the operator sets `PATH_SCRATCH` to (e.g. `/scratch`).

## Manifest Format

```json
{
  "name": "greengenes2",
  "version": "2024.09",
  "kind": "sequence_reference",
  "files": {
    "sequences": {
      "path": "seqs.fna.gz",
      "sha256": "..."
    },
    "taxonomy": {
      "path": "taxonomy.tsv.gz",
      "sha256": "..."
    },
    "backbone_phylogeny": {
      "path": "backbone.nwk",
      "sha256": "..."
    },
    "placements": {
      "path": "placements.jplace",
      "sha256": "..."
    },
    "genome_mapping": {
      "path": "genome_mapping.tsv",
      "sha256": "..."
    }
  }
}
```

Only `sequences` is required. All other files are optional — the ingestion pipeline adapts based on what the manifest declares.

## File Formats

### sequences (required)
Standard FASTA. Headers are sequence identifiers (one per line, no wrapping).

### taxonomy
Tab-separated, two columns: `feature_id<TAB>lineage`. Lineage is semicolon-separated with GTDB-style rank prefixes: `d__; p__; c__; o__; f__; g__; s__`. Empty ranks appear as the bare prefix (e.g., `s__`).

### backbone_phylogeny
Newick format. Tip names must match identifiers in the sequences file.

### placements
jplace format (Matsen et al. 2012). Placement edges reference the backbone phylogeny.

### genome_mapping
Tab-separated, three columns: `feature_id<TAB>genome_source<TAB>genome_source_id`. Maps sequence identifiers to external genome accessions. `genome_source` is one of: `genbank`, `refseq`, `collaborator`, `qiita`. `genome_source_id` is the external accession.

## Host references and the rype index

A **host reference** (`qiita.reference.is_host = true`) is an ordinary `sequence_reference` used as a **negative filter** for host-read removal: at filter time its built index is passed as rype's `negative_index` and matching reads are dropped. `is_host` is orthogonal to `kind` and is set once at creation; **taxonomy is required** for a host reference (the rype mapping authority's source), phylogeny is not.

Host references are ingested by the **`host-reference-add`** workflow (`workflows/host-reference-add/1.0.0.yaml`), driven by `qiita reference load --host --taxonomy …`. It runs the same hash → mint → write-membership → load steps as `reference-add`, then a `build_rype_index` native step, then `register-files`, then `register-index`. The status lifecycle gains an `indexing` state: `loading → indexing → active` (plain `reference-add` stays `loading → active`).

`build_rype_index` runs **before** `register-files`: `register-files` *moves* the feature-keyed `reference_sequence_chunks` staging files into permanent DuckLake storage (data-plane `move_file`), so the index build must read them from staging first.

### `.ryxdi` index layout and location

The index is a miint rype `.ryxdi` — a **directory** (manifest + Parquet shards), not a single file — written by `build_rype_index` to a persistent path on the shared filesystem (NOT the ephemeral work-ticket workspace):

```
{PATH_SCRATCH}/references/{reference_idx}/rype/index.ryxdi/
├── manifest.toml        # authoritative build manifest: buckets, k/w, salt
└── *.parquet            # bucket + inverted-shard content
```

Build defaults are **k=64, w=25** (rype's own `w` default is 50; the job passes 25 explicitly), all features mapped to a single bucket named `reference_{reference_idx}` by default. `build_rype_index` reads `PATH_SCRATCH` via the orchestrator settings and `mkdir`s the directory at runtime — no operator pre-creation needed. (On SLURM the backend propagates `PATH_SCRATCH` into the job env so the compute node resolves the real value, not the `$TMPDIR/qiita` default.)

The `register-index` action records the result in `qiita.reference_index` (`index_type='rype'`, `fs_path`, `params={k, w, bucket_name}`, `created_at`); `GET /reference/{reference_idx}/index` lists it. The authoritative manifest lives inside the `.ryxdi`; `params` is only a small copy. The table has no `UNIQUE(reference_idx, index_type)` — a future "grow a reference" can append a newer generation, and newest wins at resolution time.
