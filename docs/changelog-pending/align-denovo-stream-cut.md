### Fixed

- **nginx no longer resets a Flight DoGet whose client has stopped reading for 60 s
  (#N).** Both Flight locations in `deploy/nginx/qiita.conf` set `send_timeout 3600s`,
  beside `grpc_read_timeout` / `grpc_send_timeout`. Without it, nginx 1.26.3 reset a
  stream about 66 s after its client paused (measured on the 443 edge and the loopback
  listener), which is what an `align-denovo` job's aligner does while it works.
- **`align-denovo` fails when its masked-read stream ends short (#N).**
  `open_read_masked_stream` asks the data plane's `count_masked` DoAction for the
  pair's read count, under the same ticket, and fails the job when the rows the
  aligner drained differ from it. Some September runs completed and registered
  alignments of only part of their sample's reads after nginx cut the stream; a
  short stream now fails the ticket whatever cut it.
