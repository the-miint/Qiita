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

A **host reference** (`qiita.reference.is_host = true`) is an ordinary `sequence_reference` used for **host-read depletion**: at filter time the `host_filter` step classifies reads against its rype index (host = any emitted match — a POSITIVE index, **not** rype's `negative_index`/`-N` mode) and re-checks the survivors against a minimap2 `.mmi` sidecar; reads matching either are dropped (paired-end: the pair drops if either mate hits). `is_host` is orthogonal to `kind` and is set once at creation; **taxonomy is required** for a host reference (the rype mapping authority's source), phylogeny is not.

Host references are ingested by the **`host-reference-add`** workflow (`workflows/host-reference-add/1.0.0.yaml`), driven by `qiita reference load --host --taxonomy …`. It runs the same hash → mint → write-membership → load steps as `reference-add`, then `build_rype_index` and `build_minimap2_index` native steps, then `register-files`, then two `register-index` actions (one per index). The status lifecycle gains an `indexing` state: `loading → indexing → active` (plain `reference-add` stays `loading → active`).

`build_rype_index` runs **before** `register-files`: `register-files` *moves* the feature-keyed `reference_sequence_chunks` staging files into permanent DuckLake storage (data-plane `move_file`), so the index build must read them from staging first.

### `.ryxdi` index layout and location

The index is a miint rype `.ryxdi` — a **directory** (manifest + Parquet shards), not a single file — written by `build_rype_index` to a persistent path on the shared filesystem (NOT the ephemeral work-ticket workspace):

```
{PATH_DERIVED}/references/{reference_idx}/rype/index.ryxdi/
├── manifest.toml        # authoritative build manifest: buckets, k/w, salt
└── *.parquet            # bucket + inverted-shard content
```

Build defaults are **k=64, w=20** (rype's own `w` default is 50; the job passes 20 explicitly, overridable per build via the `rype_w` action_context key), all features mapped to a single bucket named `reference_{reference_idx}` by default. `build_rype_index` reads `PATH_DERIVED` via the orchestrator settings and `mkdir`s the directory at runtime — no operator pre-creation needed. (On SLURM the backend propagates `PATH_DERIVED` into the job env so the compute node resolves the real value, not the `$TMPDIR/qiita/derived` default.)

The `register-index` action records the result in `qiita.reference_index` (`index_type='rype'`, `fs_path`, `params={k, w, bucket_name}`, `created_at`); `GET /reference/{reference_idx}/index` lists it. The authoritative manifest lives inside the `.ryxdi`; `params` is only a small copy. The table has no `UNIQUE(reference_idx, index_type)` — a future "grow a reference" can append a newer generation, and newest wins at resolution time.

### `.mmi` minimap2 sidecar

Alongside the rype index, `build_minimap2_index` writes a minimap2 short-read index to `{PATH_DERIVED}/references/{reference_idx}/minimap2/index.mmi` (a single FILE, cleared with `unlink` on a rebuild). It consumes the **same** feature-keyed `reference_sequence_chunks` as `build_rype_index`, reassembling whole contigs per `feature_idx` via `string_agg(chunk_data ORDER BY chunk_index)` — the index is built from exactly the bytes stored in the data plane, with no raw-FASTA side channel. Because it reads those chunks, it shares the rype builder's `register-files` ordering constraint: both run **before** `register-files` moves the staging chunks. The second `register-index` records it (`index_type='minimap2'`, `fs_path`, `params={preset, source_chunks, num_subjects}`). The `host_filter` step (`fastq-to-parquet/1.1.0`) consumes the rype `.ryxdi` and this `.mmi` together — rype classify first, minimap2 on the survivors.
