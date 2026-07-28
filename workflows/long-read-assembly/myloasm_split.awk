# Split myloasm's `assembly_primary.fa` into circular (LCG) and linear (noLCG)
# multi-FASTAs. Driven by assemble.sh; also run directly by
# tests/test_myloasm_split.py, which is why it is a FILE and not inlined there —
# the split is the one part of the myloasm branch that can be proven by running
# it, so it is kept runnable without an image.
#
# Required -v arguments: circ_out, nolcg_out (output paths).
#
# WHY THE HEADER AND NOT THE GFA
# ------------------------------
# hifiasm-meta encodes circularity in its GFA segment NAME (`…tg……c` circular /
# `…l` linear), which is what the hifiasm branch splits on. myloasm does NOT: it
# states circularity directly in the assembly_primary.fa header, and its GFA
# segment names carry no such marker. Applying the hifiasm `tg[0-9]+c$` regex to
# myloasm output would match nothing, silently sending every circular genome to
# binning and losing the LCG class outright. So this reads the FASTA header.
#
# Probed on myloasm 0.6.0 (bioconda) against one real genome — M. genitalium G37
# (NC_000908.2), a complete circular chromosome — sampled twice into read sets
# identical in every respect EXCEPT wrap-around, then assembled separately:
#
#   reads sampled WITH wrap:  >u713ctg_len-580076_circular-yes_depth-32-32-32_duplicated-no mult=1.00
#   reads sampled WITHOUT:    >u278ctg_len-577882_circular-no_depth-33-33-33_duplicated-no mult=1.00
#
# Topology was the only variable, so the `circular-` field does track it.
#
# WHAT COUNTS AS CIRCULAR
# -----------------------
# ONLY `circular-yes`. myloasm also documents `circular-possibly` (a self-loop
# that fails its depth/connectivity criteria); that value was NOT reproduced by
# the probe above — low-depth runs (1-3x) all reported `circular-no` — so it is
# accepted as a well-formed value but routed to noLCG. That is both the safe
# default (a mis-called linear contig is still recovered through binning, whereas
# a mis-called circular one bypasses binning entirely) and what the assay owner
# does by hand today.
#
# WHY THE ID IS TRUNCATED AT `_len-`
# ----------------------------------
# The header's first token is the WHOLE decorated string
# (`u713ctg_len-580076_circular-yes_depth-…_duplicated-no`), and miint's
# read_fastx takes a record id to be exactly that first token — so whatever is
# left here becomes the LCG bin_id in assembly_hash. `_len-<N>` drifts by a few
# bp between re-assemblies of the same sample because the rotational start of a
# circular contig moves, which would make the bin_id unstable across runs. Cut at
# `_len-` and the bare unitig id (`u713ctg`) survives.

function die(msg) {
    printf("myloasm_split: %s\n", msg) > "/dev/stderr"
    # 64 = EX_USAGE, the exit code every entrypoint in this workflow uses for a
    # contract violation the step cannot proceed past.
    bad = 1
    exit 64
}

BEGIN {
    if (circ_out == "" || nolcg_out == "") {
        die("circ_out and nolcg_out are both required (-v)")
    }
    out = ""
}

/^>/ {
    # $1 is the header's first token; myloasm appends a SPACE-separated `mult=…`
    # field after it, so never use $0 as the id.
    id = substr($1, 2)
    if (id == "") {
        die("record " NR ": empty sequence id")
    }

    # Fail LOUD on a header shape we have not probed rather than guessing. The
    # silent failure this prevents: if a myloasm upgrade renames or reorders these
    # fields, nothing matches `_circular-yes`, circular.fa comes out empty, and
    # every closed genome is quietly demoted to binning input with no error
    # anywhere. Pinned to the three documented values so an unknown fourth also
    # stops the step.
    if (id !~ /_len-[0-9]+_circular-(yes|no|possibly)_/) {
        die("record " NR " (" id ") does not match the probed myloasm 0.6.0 header " \
            "shape <id>_len-<N>_circular-<yes|no|possibly>_… — re-probe the header " \
            "format against the pinned myloasm version before trusting this split")
    }

    circular = (id ~ /_circular-yes_/)

    sub(/_len-[0-9]+_circular-.*$/, "", id)
    if (id == "") {
        die("record " NR ": sequence id is empty once the _len-… decoration is cut")
    }
    # An LCG's bin_id IS its contig id (assembly_hash COALESCEs it from the
    # record), and read_id is `kind:bin_id:contig_id` — so a duplicate id would
    # collapse two distinct genomes onto one identity downstream.
    if (id in seen) {
        die("duplicate contig id '" id "' at record " NR " (first seen at record " seen[id] ")")
    }
    seen[id] = NR

    out = circular ? circ_out : nolcg_out
    print ">" id > out
    next
}

{
    # Sequence lines, copied VERBATIM and one at a time, so a line-wrapped FASTA
    # is handled without ever holding a whole contig in memory. myloasm 0.6.0 was
    # observed to write each contig on a single line, but nothing here relies on
    # that.
    if (out == "") {
        die("sequence data at line " NR " before any '>' header")
    }
    print $0 > out
}

END {
    if (bad) {
        exit 64
    }
    # Both files must exist even when a class is empty: the hifiasm branch always
    # creates both, and assemble.sh pre-creates them, so an empty class stays an
    # empty file rather than a missing one.
}
