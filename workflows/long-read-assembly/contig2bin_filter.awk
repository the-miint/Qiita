# Reduce `Fasta_to_Contig2Bin.sh` output to the candidate bins DAS_Tool should
# score. Run by bin_refine.sh, once per binner:
#
#     Fasta_to_Contig2Bin.sh -i <binner>_bins -e fa \
#       | awk -v rejects=<path> -f contig2bin_filter.awk > <binner>.tsv
#
# Its own file rather than inline in bin_refine.sh so a test can execute it.
#
# THE BIN ID IS COLUMN 2
# `Fasta_to_Contig2Bin.sh -e fa` writes <contig>\t<filename minus ".fa">: two
# tab-separated fields, no header. Measured against the shipped
# long-read-assembly-dastool SIF over all three binners' output — 4,910 concoct +
# 4,910 metabat2 + 2,930 maxbin2 rows, every one NF==2. metabat2's table was read
# from $4, which is empty at that width, so every row carried the empty string as
# its bin id: DAS_Tool saw one unnamed bin holding the whole assembly and scored
# it out. Across 60 production runs it selected 6,023 MaxBin and 201 CONCOCT bins
# and 0 MetaBAT.
#
# WHAT COUNTS AS A BIN
# metaWRAP names every real bin `bin.<N>.fa`, in all three binners' dirs, and each
# binner's catch-all outside that shape: metabat2 writes bin.unbinned.fa /
# bin.tooShort.fa / bin.lowDepth.fa, concoct writes unbinned.fa, maxbin2 writes
# none (measured on the same run: 112 / 30 / 58 files). A catch-all holds the
# contigs the binner did NOT place, so passing one on asks DAS_Tool to score the
# residue as a genome — concoct's held 3,326 of that run's 4,910 contigs.
#
# A row in neither shape is written to `rejects` and emitted nowhere; bin_refine.sh
# fails the step on a non-empty rejects file. Keeping an unrecognized id would put
# a catch-all back in front of DAS_Tool, and dropping it would take a real bin out
# of the consensus, so neither is done without a person looking.
BEGIN {
    FS = OFS = "\t"
    if (rejects == "") {
        print "contig2bin_filter: -v rejects=<path> is required" > "/dev/stderr"
        exit 64
    }
    split("bin.unbinned bin.tooShort bin.lowDepth unbinned", catch_all, " ")
    for (i in catch_all) drop[catch_all[i]] = 1
}
NF == 2 && $2 ~ /^bin\.[0-9]+$/ { print $1, $2; next }
NF == 2 && ($2 in drop) { next }
{ print > rejects }
