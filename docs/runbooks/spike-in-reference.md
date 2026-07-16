# Loading a SynDNA / spike-in reference

How to load a SynDNA (or any spike-in) reference so
`qiita submit-host-filter-pool --syndna-reference-idx <idx>` accepts it. The
submit gate (`_assert_host_reference_ready`) requires the reference to be
**ACTIVE and carry a `minimap2` index** — nothing else.

## Which FASTA — plasmids, not bare inserts

The `read-mask` `syndna` step aligns reads to the reference with minimap2 and
counts a read as a spike-in when it passes an **aligned-fraction gate**
(`MIN_ALIGNED_FRACTION = 0.90`, `MIN_IDENTITY = 0.95`; `_coverage.py`). That
fraction is `cigar_query_coverage` — the fraction of the **read** that aligned —
and it is **only correct against the full plasmids**: a read spanning the
insert→backbone junction aligns end-to-end against the plasmid (fraction ~1.0)
but only ~0.60 against the bare insert window, so the same 0.90 gate would drop
genuine spike-in reads. **Load the plasmids FASTA** (e.g.
`AllsynDNA_plasmids_FASTA_ReIndexed_FINAL.fasta`), not the bare-inserts one.
(Per-insert *quantification* — a deferred consumer — is the inserts-plus-GFF3
path; that is separate from masking.)

## Taxonomy — a Parquet with a GTDB-prefixed lineage (not a TSV)

`qiita reference load --host` **requires `--taxonomy`** even with
`--no-rype-index` (the loader rejects `--host` without it, and the
`host-reference-add` workflow lists `taxonomy_upload_idx` as required). For a
**remote (DoPut)** load the taxonomy must be a **Parquet** file — the runner
streams it through `_passthrough_parquet_stream`, which reads Parquet, not TSV —
with two columns:

- `feature_id` — must match the FASTA record headers (first whitespace-delimited
  token), the key the loader joins on.
- `taxonomy` — a semicolon-separated, **GTDB rank-prefixed** lineage
  (`d__; p__; c__; o__; f__; g__; s__…`), **≤ 8 ranks, no blank fields** (an
  empty rank is the bare prefix `d__`, not `""`). The loader hard-validates the
  prefix order.

SynDNA inserts are **synthetic constructs** — NCBI places them under *artificial
sequences* (`32630`). Do **not** put the construct at the species rank with empty
higher ranks: a reference taxonomy must populate EVERY parent rank (a species
implies a genus implies a family, …), so an empty-parent lineage is a hierarchy
violation. Use an artificial lineage, one species per construct so the inserts stay
distinct:

```
d__Artificial; p__Artificialota; c__Artificialia; o__Artificiales; f__Artificialaceae; g__synDNA; s__synDNA <id>
```

Because `--no-rype-index` skips the rype build, this taxonomy is stored as feature
metadata only (not a classification authority) — but keep the hierarchy valid so a
consumer that later DOES read it isn't handed a species with no genus.

Generate it from the FASTA headers with pyarrow (one species per construct, keyed
by its `feature_id`):

```python
import pyarrow as pa, pyarrow.parquet as pq
ids = [l[1:].split()[0] for l in open("plasmids.fasta") if l.startswith(">")]
_prefix = "d__Artificial; p__Artificialota; c__Artificialia; o__Artificiales; f__Artificialaceae; g__synDNA"
tax = [f"{_prefix}; s__synDNA {i}" for i in ids]
pq.write_table(
    pa.table({"feature_id": pa.array(ids, pa.string()),
              "taxonomy":  pa.array(tax, pa.string())}),
    "syndna_taxonomy.parquet",
)
```

## Why `--host` for a spike-in (it isn't a host)

Only the `host-reference-add` workflow builds a **whole-reference minimap2
`.mmi`** — `reference-add` builds no aligner index, and `--shard-index` builds
per-shard indexes via the (data-plane-streaming) sharded path. So `--host
--no-rype-index --minimap2-preset map-hifi` is the sanctioned way to get a
map-hifi `.mmi`. The reference gets `is_host = true` as a side effect, but it is
**never auto-selected as a host filter** — host filtering resolves per sample via
`host_taxon_id → host_filter_profile` (by name), and the spike-in reference is in
no profile. It is used only because you pass `--syndna-reference-idx` explicitly.
Cosmetic effects: it appears under `reference list --host`, and its load ticket /
email is named `host-reference-add`.

## The command

From a machine with the FASTA + your PAT (remote DoPut through the public TLS
edge; or `--local` on-host with a manifest):

```bash
qiita --base-url https://<host> reference load \
  --name synDNA-plasmids --version 1.0 \
  --fasta /path/AllsynDNA_plasmids_FASTA_ReIndexed_FINAL.fasta \
  --taxonomy /path/syndna_taxonomy.parquet \
  --host --no-rype-index --minimap2-preset map-hifi \
  --data-plane-url grpc+tls://<host>:443
```

It watches to completion (`loading → indexing → active`). If the FASTA + taxonomy
minted the reference but a later step failed, re-run bound to the existing row
with `--reference-idx <idx>` instead of `--name/--version` (which 409s "already
exists").

## Verify + use

```bash
qiita --base-url https://<host> reference list --active --index-type minimap2   # must list it with ['minimap2']
```

Then submit: `qiita submit-host-filter-pool --sequencing-run-idx R \
--sequenced-pool-idx P --syndna-reference-idx <idx>` (PacBio absquant pools
require it; it is rejected on a non-absquant pool).
