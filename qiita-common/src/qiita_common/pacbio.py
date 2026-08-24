"""PacBio SMRT run-folder layout: locating a run's demultiplexed HiFi BAMs.

The PacBio counterpart to `qiita_common.illumina`. Both are here rather than in
the CLI because the control plane reads the run folder too — the
`/run-folder/inspect` route is what lets a submit run from a machine that does
not mount the cluster, and it has to index the folder exactly the way the
client used to.
"""

from __future__ import annotations

from pathlib import Path

# Per-cell reads PacBio's demux could not assign to a barcode; never a sample.
UNASSIGNED_BAM_SUFFIX = ".unassigned.bam"

# Per-SMRT-cell subdirectory holding demultiplexed HiFi BAMs, and the filename
# field that names it: `{run}/{well}/hifi_reads/{movie}.hifi_reads.{barcode}.bam`.
HIFI_READS_DIR = "hifi_reads"


def index_run_bams(run_folder: Path) -> tuple[dict[str, Path], set[str]]:
    """Index a PacBio run folder's per-barcode HiFi BAMs.

    Globs `{run_folder}/*/hifi_reads/*.bam` — each SMRT cell is a well
    subdirectory (`1_A01`, `1_B01`, ...) holding its demultiplexed reads — and
    keys each BAM on its barcode, the second-to-last dot field of the filename
    (`m84137_..._s1.hifi_reads.bc2073.bam` -> `bc2073`). Per-cell
    `*.unassigned.bam` files are skipped (reads with no barcode are not samples).

    Returns `(index, duplicated)`: `index` maps barcode -> BAM for every barcode
    that resolves to exactly one file; `duplicated` is the set of barcodes seen
    under more than one SMRT cell. A duplicated barcode is left OUT of `index` and
    is a hard error at resolution time — barcode reuse across SMRT cells within a
    run is real (e.g. bc2083 under both 1_B01 and 1_C01) and cannot be
    disambiguated without the SMRT cell. This is the graceful-degradation rule:
    unique barcodes just resolve; a collision on a barcode a sample actually needs
    fails loud rather than silently binding the wrong cell's reads. (The preflight
    now carries a SMRT-cell field; once it is populated, key on `(smrt_cell, barcode)`
    — matching the well subdirectory or the movie name's `s#` token — and this
    collision set becomes empty.)
    """
    index: dict[str, Path] = {}
    duplicated: set[str] = set()
    for bam in sorted(run_folder.glob(f"*/{HIFI_READS_DIR}/*.bam")):
        if bam.name.endswith(UNASSIGNED_BAM_SUFFIX):
            continue
        parts = bam.name.split(".")
        # Require the exact demux shape "<movie>.hifi_reads.<barcode>.bam" so a
        # non-demuxed combined BAM ("<movie>.hifi_reads.bam") isn't indexed under a
        # spurious barcode ("hifi_reads"). ["m84_s1", "hifi_reads", "bc2073", "bam"].
        if len(parts) < 4 or parts[-3] != HIFI_READS_DIR:
            continue
        barcode = parts[-2]
        if barcode in index or barcode in duplicated:
            duplicated.add(barcode)
            index.pop(barcode, None)
        else:
            index[barcode] = bam
    return index, duplicated
