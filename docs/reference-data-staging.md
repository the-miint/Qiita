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
