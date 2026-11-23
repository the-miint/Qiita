### Fixed

- **nginx no longer resets a Flight DoGet whose client has stopped reading for 60 s
  (#936).** Both Flight locations in `deploy/nginx/qiita.conf` set `send_timeout 3600s`,
  beside `grpc_read_timeout` / `grpc_send_timeout`. Without it, nginx 1.26.3 reset a
  stream about 66 s after its client paused (measured on the 443 edge and the loopback
  listener), which is what an `align-denovo` job's aligner does while it works.
- **A data-plane stream that ends short fails instead of being used (#936).** Every
  DoGet, in a job, the control plane, `qiita` (reference, assembly and feature-table
  exports and builds) and `qiita-admin masked-read-export`, first asks the new
  `count_doget` DoAction how many rows the same ticket streams, and fails when the
  stream delivered a different number. `qiita-admin masked-read-export` compares an
  existing Parquet with that count too, in place of `count_masked`, which also counted
  a passing read whose stored read is missing.
  `align-denovo` also fails when the aligner did not read every read. Before this,
  some `align-denovo` runs completed and registered alignments of only part of their
  sample's reads after nginx cut the stream; a short stream now fails the ticket
  whatever cut it. After this deploy, the de novo samples completed before it are re-run
  under the same `alignment_idx` where their alignment's params allow it (the rest are
  listed for follow-up), and feature tables built from them before the re-run need
  rebuilding.

### Added

- **A job's step log records how long it left each stream unread (#936):** the wait
  before its first batch and its longest hold of a batch, the two waits nginx's
  `send_timeout` has to cover.
