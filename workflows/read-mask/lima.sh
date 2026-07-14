#!/bin/bash
# Clip the Twist adapter from each end of a sample's HiFi reads.
#
# Input  `lima_in_fastq`  — one record per read, the FASTQ record NAME being the
#                           read's `sequence_idx` (written by jobs/lima_export.py).
#        `lima_config`    — {"args": "--hifi-preset ASYMMETRIC --neighbors …"},
#                           the control-plane-resolved argument string. A scalar
#                           cannot ride a container step's inputs (the runner
#                           would treat it as a bind-mount path), so it arrives as
#                           a file — the same trick long-read-assembly's
#                           assembly_run_config step uses for its `assembler`.
# Output `lima_out_fastq` — the surviving reads, adapter-clipped, names preserved.
#
# lima preserves the record name VERBATIM and appends its BAM tags after a single
# space, so `sequence_idx` round-trips and miint's `read_fastx` parses the tags
# into a separate `comment` column. Reads lima drops simply do not appear in the
# output; jobs/lima_mask.py turns their absence into a `twist_no_adaptor` mask row
# via `infer_trim`, which returns NULL/NULL for an omitted read.
#
# The adapter FASTA is baked into the image (see lima.def) and its path is
# exported as QIITA_LIMA_ADAPTER_FASTA. Its RECORD ORDER is load-bearing: the
# resolved args include `--neighbors`, which emits a read only when its
# best-scoring barcode pair are adjacent records in the file.
source /opt/qiita/_lib.sh

READS_FASTQ="$(qiita_input lima_in_fastq)"
LIMA_CONFIG="$(qiita_input lima_config)"
LIMA_ARGS="$(jq -er '.args' "${LIMA_CONFIG}")"

if [[ ! -f "${QIITA_LIMA_ADAPTER_FASTA}" ]]; then
    echo "adapter FASTA missing from the image: ${QIITA_LIMA_ADAPTER_FASTA}" >&2
    exit 64
fi

OUT="${QIITA_OUTPUT_PATH}/lima_out.fastq"

# LIMA_ARGS is a control-plane-resolved constant (never client-supplied — the only
# client knob is `lima_preset`, mapped to this string by a CP table), so bare word
# splitting is intended: the flags must reach lima as separate argv entries.
# shellcheck disable=SC2086
micromamba run -n lima lima \
    "${READS_FASTQ}" \
    "${QIITA_LIMA_ADAPTER_FASTA}" \
    "${OUT}" \
    --num-threads "${THREADS}" \
    ${LIMA_ARGS}

# lima writes an empty FASTQ when every read fails adapter detection. That is a
# legitimate outcome, not an error: every read becomes `twist_no_adaptor` in the
# mask. Leave the empty file for lima_mask to interpret; do not exit non-zero.
if [[ ! -f "${OUT}" ]]; then
    echo "lima produced no output file at ${OUT}" >&2
    exit 70
fi

qiita_finish lima_out_fastq=lima_out.fastq
