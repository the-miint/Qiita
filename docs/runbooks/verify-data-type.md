# Verifying a prep_sample's data type

For admins and operators. How to load the four rRNA marker references and submit `verify-data-type`, which
samples up to 10,000 of a prep_sample's raw reads, classifies them with SortMeRNA and
writes one `data_type` QC check. The check's `metrics` carry the share of reads per
class, and its verdict comes from the prep_sample's protocol (thresholds below, in
`qiita_compute_orchestrator.jobs.verify_data_type`).

| Protocol | Verdict |
|---|---|
| any thresholded protocol, under 1,000 sampled reads | `info`, `insufficient_reads` |
| `short_read_amplicon`, `long_read_amplicon` | under 0.5 of sampled reads classified: `info`, `no_rrna_marker`; otherwise `fail`, `data_type_mismatch` when the top class holds under 0.90 of the classified reads |
| `short_read_metagenomics`, `long_read_metagenomics` | `fail`, `data_type_mismatch` at 0.05 or more of sampled reads classified |
| `short_read_transcriptomics` | `info` |

`no_rrna_marker` is the expected result for amplicons that are not rRNA, such as ITS. A
read is classified when it aligns to a class's reference with at least 50% query coverage.

## Reference databases

SortMeRNA 4.3.7 (git tag `v4.3.7`, commit `307e84021ab3561c38fbee2854bef3eba8e6cfda`),
directory `data/rRNA_databases/`. The release's `smr_v4.3_*_db.fasta.gz` bundles each
concatenate every class into one file, so they cannot give one reference per class; use
the per-class files they were built from. The same files are in `v4.3.6`.

| Class | File (SHA-256) | Size (bytes) |
|---|---|---|
| 16S | `silva-bac-16s-id90.fasta` (`b3a59c1ca2ec98516ee4954e68edcaaab3c56f87fb48ec867dafda0d72e83586`) | 19437013 |
| 16S | `silva-arc-16s-id95.fasta` (`f13340107aa2f2b38f9d938e95b87b66446dc4bb064d961d4994de9e744fb37f`) | 3893959 |
| 18S | `silva-euk-18s-id95.fasta` (`6207954b055e6c38ad8ac9139ef883f04c08f0b23c4d6b7afabbc7ec9a089dc9`) | 13259584 |
| 23S/28S | `silva-bac-23s-id98.fasta` (`a568b5231afe2bcb5945c572efda9ca108544e7b76ff854072f63af52007e959`) | 12911743 |
| 23S/28S | `silva-arc-23s-id98.fasta` (`a3a7a743088e0e2e07aff9a0a8b327a40043a35bb3c4e9b8d93c3fa6bc411651`) | 752022 |
| 23S/28S | `silva-euk-28s-id98.fasta` (`b0dacf3b305c131af7ca7da48cddd9e53237da0fe89f789e9bb60a7febf03bba`) | 14945070 |
| 5S/5.8S | `rfam-5s-database-id98.fasta` (`83b8837743e30469e89a2e5cedc29311b4662152b79c683fe09161d1200eadd0`) | 8525326 |
| 5S/5.8S | `rfam-5.8s-database-id98.fasta` (`e5c0d4fcd572591427fd18a0877350e3e65b42e33d5ccb8cf6c5338e9e8cb2e8`) | 2280449 |

The `-idNN` suffix is the clustering identity: the files hold cluster centers only, which
keeps the data-plane fetch at run time small. The SILVA files derive from SILVA release
119 (SSURef NR99 and LSURef). The 5S and 5.8S files are pinned by the SortMeRNA commit
above; its README does not name an Rfam release.

Fetch each file from
`https://raw.githubusercontent.com/sortmerna/sortmerna/v4.3.7/data/rRNA_databases/<file>`
and check its SHA-256 before loading.

## Load the four references

A class with several files takes a manifest of absolute paths, one per line. Run on a
host that sees the files:

```bash
printf '%s\n' /data/smr/silva-bac-16s-id90.fasta /data/smr/silva-arc-16s-id95.fasta > 16s.manifest
qiita --base-url https://<host> reference load --local \
  --name sortmerna-marker-16s --version 4.3.7 --fasta-manifest "$PWD/16s.manifest"
```

Repeat with `sortmerna-marker-18s`, `sortmerna-marker-23s-28s` and
`sortmerna-marker-5s-5.8s`, each with its own manifest. The load watches to `active`;
record each `reference_idx`, or find them with
`qiita --base-url https://<host> reference list --active`.

## Submit

One ticket per prep_sample, with the four idx values in the context:

```bash
qiita --base-url https://<host> ticket submit \
  --action-id verify-data-type --action-version 1.0.0 --prep-sample-idx <P> \
  --context-json '{"marker_16s_reference_idx": <a>, "marker_18s_reference_idx": <b>,
    "marker_23s_28s_reference_idx": <c>, "marker_5s_5_8s_reference_idx": <d>}'
```

The runner reads the protocol from the prep_sample; a prep_sample with no reads ends
`no_data` first, and otherwise a protocol with no threshold fails the ticket. Read the result from `GET /qc-check/{prep_sample_idx}`: the `data_type` row's
`metrics.marker_reference_idx` records which references were used. A re-run replaces the
row. A prep_sample with fewer than 1,000 reads gets `insufficient_reads`; one with no reads
ends `no_data` and writes nothing.

The job exports the prep_sample's raw reads, so submitting needs an admin role.
