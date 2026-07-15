"""The shared spike-in gate: thresholds and CIGAR-derived scoring expressions.

Single-sourced here so the read-mask side (`syndna`, which masks a read as a spike-in) and
a future coverage-measurement consumer (which counts a read toward depth) evaluate ONE
predicate. If the two ever used different cutoffs a read could be called a spike-in and yet
contribute no depth (or vice versa), silently. Today `syndna` is the only importer; the
module is factored out precisely so the measurement caller, when it lands, cannot diverge
from it — neither redefines these.

**The gate runs on the UNSLICED alignment.** Reads are aligned to the PARENT (the whole
plasmid, not the bare insert), and the identity + aligned-fraction filters are applied
there, before any windowing. A read spanning the insert->backbone junction is a REAL
spike-in molecule: against the plasmid it aligns end-to-end (aligned fraction 1.0) and
passes; against the insert *window* it is only ~60% aligned, so the same 0.90 filter applied
after windowing would delete exactly the reads a plasmid-level reference exists to keep.
(Measured on the real SynDNA plasmids: 0.60.)
"""

from __future__ import annotations

# Scoring the alignment. Both are computed from the CIGAR alone — no NM/MD tags. On
# minimap2's eqx CIGAR, `cigar_sequence_identity` reproduces blast identity exactly.
IDENTITY_EXPR = "cigar_sequence_identity(cigar)"
ALIGNED_FRACTION_EXPR = "cigar_query_coverage(cigar)"

# The gate's THRESHOLDS, single-sourced here alongside its expressions. Settled with the
# assay owner: a read contributes iff it is >= 95% identical AND >= 90% of it aligns, both
# measured against the whole PLASMID (pre-window).
MIN_IDENTITY = 0.95
MIN_ALIGNED_FRACTION = 0.90

# Only primary, mapped alignments contribute. Primary-only is load-bearing rather than
# tidy: the plasmid BACKBONE is identical across every SynDNA plasmid, so one read can
# align to several of them at high identity. Counting secondaries would multi-count the
# same molecule across inserts.
#
# `alignment_is_primary` is TRUE for an UNMAPPED read (the SAM spec makes unmapped
# implicitly primary), so the second conjunct is not redundant. miint has since added
# `alignment_is_mapped_primary` for exactly this confusion; adopt it once the mirror build
# carries it.
MAPPED_PRIMARY_EXPR = "alignment_is_primary(flags) AND NOT alignment_is_unmapped(flags)"
