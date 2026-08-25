# Arrow Flight Surface

## Client Interfaces (Unresolved)

Client interfaces are the user-facing layer through which researchers and systems interact with Qiita. All interfaces authenticate via AuthRocket (OAuth2/JWT) and connect through the nginx gateway.

**Candidate interfaces** (to be discussed):
- **CLI** — command-line tool for scripted/automated workflows. Would speak both REST (control plane) and Arrow Flight (data plane) for data upload/download.
- **Web application** — browser-based UI for study management, search, and monitoring. REST-only (control plane). Bulk data transfer would be delegated to CLI or SDK.
- **Python/R SDK** — programmatic library for use in analysis scripts and pipelines. Would wrap both REST and Arrow Flight APIs, providing native DataFrame integration (pandas, polars, R data.frame).
- **Notebook integration** — Jupyter/RStudio integration, likely built on top of the SDK.

## Arrow Flight Operations (no custom .proto needed)

- **DoGet**: select by key (table + integer identifiers encoded in signed Flight ticket)
- **DoPut**: upload data (stream RecordBatches to shared filesystem via FlightDescriptor, authorized by signed action token)
- **DoAction**: register file (`ducklake_add_data_files`), delete by key, insert-from-processing-method (authorized by signed action token)

### DoGet stream compression

A DoGet client may ask for a zstd-compressed Arrow IPC body by sending the
`qiita-ipc-compression: zstd` gRPC metadata header. **The default is
uncompressed**, and an unrecognised value is rejected rather than ignored —
a client that asked for compression and silently did not get it would draw the
wrong conclusion about its own transfer. `zstd` is the only codec offered; LZ4
measured at roughly half its ratio on every production shape.

**Off by default is deliberate, and the reason is not obvious: compression makes
a DoGet *slower* over a fast link.** Break-even is
`bandwidth = encode_rate × (1 − 1/ratio)`, which measurement puts at ~4 Gbit/s —
a 775 MiB alignment stream takes 0.65 s uncompressed over 10 GbE against 1.53 s
with zstd, but 6.5 s against 2.9 s over 1 GbE. Every in-repo caller sits above
that line (the control-plane runner reaches the data plane over loopback,
compute jobs over the cluster fabric), so none of them sends the header. It is
for clients on slow links, and `qiita-admin masked-read-export` exposes it as
`--compress` because that is the DoGet whose payload is largest. It is not the
only off-site DoGet — `qiita reference genome-export` (`cli/user/reference.py`)
is a user-facing client and streams too; it does not opt in yet, and the flag is
the pattern to copy when it should.

The server cannot choose this itself: the deciding input is the *client's*
bandwidth, and behind nginx the data plane cannot even see the client's address.

**gRPC's own `grpc-accept-encoding` negotiation is not an alternative, and the
reason is the client, not the protocol.** tonic can compress a whole gRPC
message (`CompressionEncoding::Zstd`, behind its `zstd` feature), which would
cover `data_body` — so on its face it is a candidate. What rules it out is that
our client cannot ask for it: capturing the HTTP/2 HEADERS frame a pyarrow Flight
client sends on a DoGet gives `grpc-accept-encoding: identity, deflate, gzip`
from `grpc-c++/1.71.0 grpc-c/46.0.0`, with no zstd, so a server offering zstd at
the transport layer would negotiate down to nothing every time. (Separately, the
IPC codec is stamped per record-batch message by the writer, which is why the
custom header lives at that layer rather than the transport's.)

Header name constants: `IPC_COMPRESSION_HEADER` in
`qiita-data-plane/src/flight_service.rs` and its twin in
`qiita-common/src/qiita_common/flight_constants.py`.

### DoGet column projection

A Flight ticket carries an optional `columns` list beside its row scope. The
control plane validates it against a per-table allowlist at mint time and signs
it; the data plane validates it again and projects exactly that set, in that
order. Validating twice is deliberate — the control plane's copy turns a typo
into a 422 with a useful message, and the data plane's is the defense-in-depth
that keeps a signed name out of interpolated SQL, the same argument
`ALLOWED_FILTER_COLUMNS` makes for the filter.

**Only `alignment_visible` is projectable, and it *requires* a column list.**
Every other DoGet table streams `SELECT *` and rejects a list rather than
ignoring one. The asymmetry is not arbitrary, and this is the one measured number
behind it: on a **HiFi** alignment payload of 716,187 rows (mean CIGAR ≈ 1,092
bytes), the Arrow stream was 775.6 MiB, of which `cigar` alone was 746.1 MiB
(96.2%) and the six identifier/position columns the feature-table consumer
actually binds were 29.5 MiB combined. Serving that unprojected is ~26x the
bytes. Two caveats the figure needs: the projection is 6 of 23 columns rather
than "everything but `cigar`", so it is not a `cigar`-only saving; and the
equivalent **short-read** shape was never measured — short-read CIGARs are
near-degenerate, so expect a much smaller share there and do not quote 96% for
it. Meanwhile the reference tables are broadly readable by design (mirroring the
anonymous REST `GET /reference/{idx}`) and narrowing them would buy nothing
measurable.

**Changing a consumer's column set is a rollout-order decision.** The data plane
must be restarted before, or with, the control plane that starts signing the new
list — `deploy/activate.sh` restarts the CP first, so the default order leaves a
brief window where a new list reaches a data plane that predates it. A data plane
of this vintage or later refuses a ticket carrying a field it does not know
(`deny_unknown_fields` on `TicketPayload`), which makes the mismatch loud; an
older one silently applied its own idea of the projection instead.

There is **no server-side default projection** — a ticket without a column list
is refused. Only the consumer knows which columns it binds, and a fallback in
the data plane would be a second answer to that question, free to drift wider
than what was asked for. The cost is that a ticket minted before a deploy and
redeemed inside its 300 s TTL after it fails with `InvalidArgument`; that is
preferred to widening it silently. The one production consumer,
`estimate_feature_table`, owns its list as `_ALIGNMENT_COLUMNS` and uses it
twice — for the ticket and for the `SELECT` it binds — so the two cannot drift.

Allowlist constants: `ALIGNMENT_PROJECTION_COLUMNS` in
`qiita-data-plane/src/flight_service.rs` and `_PROJECTION_COLUMNS` in
`qiita-control-plane/src/qiita_control_plane/auth/tickets.py`. Both are
hand-copies (neither language can import the other) pinned by parity tests, and
the Rust one is additionally checked against the live `alignment_visible`
schema.

### Two mint paths for the alignment DoGet

`alignment_visible` is the one Flight surface with two ticket-minting routes, and
they differ in **where the cohort comes from**, which is what forces two
authorization models:

| Route | Caller | Cohort source | Authorization |
|---|---|---|---|
| `POST /alignment/ticket/doget` | service account, `ticket:doget` | the work ticket's `action_context` | scope only — the runner resolver already validated the cohort at submit |
| `POST /alignment/{alignment_idx}/ticket/doget` | human, `alignment:doget` | the request body | `Tier.VIEWER` on every study each `prep_sample_idx` links to, all-or-nothing, plus a completeness re-check |

The second exists because a scientist pulling their own data has no runner
upstream to have validated anything. It is also the only place that validation
can happen: **the signed cohort is the authorization boundary.** The data plane
verifies the signature and serves exactly the identifiers the ticket names — it
holds no notion of studies, users, or tiers, by the same design that makes it
Intentionally "dumb" in the component map above. There is no second line of
defence behind the mint, so a control-plane bug that signs an unauthorized
`prep_sample_idx` is a data leak, not a caught error.

Two consequences worth stating, because both look like over-engineering until
you know why:

- **The human mint refuses a partially-readable cohort rather than narrowing
  it.** Coverage filtering makes a feature table cohort-dependent, so a quietly
  trimmed cohort answers a different scientific question under the name of the
  one that was asked — and the bundle manifest would record a cohort the caller
  never requested. The paired *discovery* reads
  (`GET /sequencing-run/{run}/sequenced-pool/{pool}/alignment[/{idx}/cohort]`) do
  narrow, because a listing carries no scientific result; between them, you
  discover exactly the cohort you are then allowed to mint.
- **Access is checked before completeness.** Reversed, the 422 naming the
  incomplete samples would tell a caller which samples are finished for an
  alignment they have no right to read at all.

### One map and three mints that are REST, not Flight tickets

Turning those alignment rows into a publishable feature table needs four
control-plane calls, and every one is REST rather than a signed Flight ticket —
the one place bulk-shaped analytic data does not come off the data plane:

| Call | Answers | Why not Flight |
|---|---|---|
| `GET /reference/{reference_idx}/genome-map` | `feature_idx` → `(genome_idx, source, source_id)` | `genome_idx` and the genome's provenance exist **only in Postgres**; no DuckLake table carries them, and `genome_idx` is not a filter column |
| `POST /exported-identifier` | `(alignment_idx, prep_sample_idx)` → `export_id` | the identifier is *minted* here; it exists in Postgres only, and creating one is a write |
| `POST /exported-feature` | `genome_idx` \| `feature_idx` → `export_feature_id` | same — a mint, and a write |
| `POST /exported-processing` | `alignment_idx` → `export_processing_id` | same |

That is the whole criterion: **a signed ticket can only name identifiers the
data plane can resolve**, and no call's columns exist out there. It is not a
policy exception and it does not generalize — anything whose columns do live in
the lake still goes through a ticket.

**The three mints are one namespace, not three conventions.** Each is a table
whose public handle is a GENERATED, UNIQUE column, so the guarantee that two
things never publish under one name is a database fact rather than a client
assertion; each is idempotent, so a rebuild renames nobody; and each retires
rather than deletes, because a published handle is a promise. `exported_feature`
is the hybrid — an accession wins wherever the entity has one no live row has
already published, and a minted `QF<n>` covers the rest — which is why a bundle
names its rows `GCF_000006605` rather than replacing a handle people know. The
shape, the rejected alternatives, and what a new *kind* must update are in the
three migrations (`20260810000000_exported_identifier.sql`,
`20260813000000_exported_feature.sql`, `20260813000001_exported_processing.sql`);
the FORWARD PLAN comment in each is the copy to read before adding one.

**The exported identifier is the boundary where our identifiers stop.** A
published table names its samples `QM<n>`, never `prep_sample_idx` — see the
opaque-identifier rule in `CLAUDE.md`. No accession can do that job: a biosample
sequenced repeatedly has several prep_samples, so its accession cannot say which
sequencing a row came from, and an ENA run accession is NULL until submission,
which may not have happened. `export_id` names a *processed* sample — the sample
plus the processing it went through — because a feature table's rows are
processing-specific, so the same sample under two alignments is two things. The
map is the only artifact carrying both `export_id` and `prep_sample_idx`; that
pairing is its entire purpose, and it is what must not be shipped onward.

**Both ship JSON, and for the genome map that is a known limit rather than an
oversight.** The genome map for a GG2-scale reference is millions of rows, so it
**refuses with a 413**
above its cap instead of truncating: a lookup table silently missing rows drops
those features from the caller's roll-up, producing a *wrong* feature table
rather than a partial one, and nobody checks a `truncated` flag on a map. The
first real reference that trips that 413 is the trigger to build the streamed
Parquet form (over the server-side cursor `export_member_genome` already uses) —
which would be the control plane's first non-JSON response body, and is worth
doing deliberately, with the held-connection cost measured, rather than
pre-emptively.

The genome map's row set is `export_member_genome`'s widened with the genome
columns, deliberately: the compute side consumes that Parquet and the client
consumes this map, so the two must not disagree about which features have
genomes. The label map is the alignment mint's sibling by the same reasoning —
same cohort shape, same `Tier.VIEWER` all-or-nothing gate, same refusal wording,
same access-checked-first ordering. Two answers to "may this caller read this
sample" is the drift that ends with one surface advertising what the other
refuses.

### The client-side feature table

`qiita feature-table build` composes the primitives above into a genome-keyed (OGU)
feature table **on the caller's own machine**. The division is the point: the control
plane authorizes, mints public identifiers, and signs tickets; the data plane streams
rows; the analytic, the relabel, and the write run in the client's own DuckDB under the
caller's own credentials. **The table itself is computed nowhere else and stored
nowhere** — it is a derived artifact on the caller's disk, not a resource this system
holds. (The `export_id`s naming its samples *are* persisted, in Postgres, because they
are promises: see the map section above.)

```
alignment slice  ───Flight DoGet (projected)──┐
reference lengths ──Flight DoGet (whole ref)──┤
reference taxonomy ─Flight DoGet (whole ref)──┤──> coverage filter ──> woltka_ogu
reference phylogeny Flight DoGet (whole ref)──┤                            │
genome map ─────────REST─────────────────────>┤                            v
exported identifiers REST (mint)─────────────>┘        exported features ──REST (mint)
                                                       exported processing REST (mint)
                                                                           │
                                                         relabel + shear <─┘
                                                                           v
   bundle: table (Parquet | BIOM) + .exported-identifier.json + .manifest.json
                          + [.taxonomy.parquet] + [.tree.parquet]
```

**The row mint is downstream of woltka, not an input to it.** It is keyed on the genomes
the roll-up actually emitted, so a build never mints a permanent public handle for a row it
does not publish (`_published_genome_idxs`).

**The analytic is SQL text in the `qiita_common.analytic` package, shared with the
compute-orchestrator's `estimate_feature_table` job.** Two consumers run the same
analytic and must not disagree about it; they differ only in where the inputs come from
and how the result is written — which is why the package owns no connection and no
streaming. Its docstrings are the single copy of *why* each step is shaped as it is;
this section is the map, not a second copy. What a reader of the pipeline needs to know is that six of
its properties are load-bearing rather than stylistic, and each is enforced and
explained at exactly one place:

| Property | Enforced at |
|---|---|
| the coverage survivor set joins **before** woltka, not after its output | `ogu_input_table_sql` |
| a CIGAR gate filters the coverage calculation as well as the counts | `gated_alignment_table_sql` |
| the relabel precedes the write and is what makes BIOM writable at all | `LABELLED_SCHEMA`, `labelled_relation_sql` |
| the coverage scope rides the *relation name*, so a scope mismatch is a bind error | `_SURVIVOR_TABLES` |
| a genome speaks through its lowest **classified** member, not its lowest | `genome_representative_taxonomy_select_sql` |
| the tree's **tips are renamed before the shear**, not its output after | `shear_input_statements` |

The last two are what make the optional companions — `--taxonomy` and `--tree` —
joinable to the table rather than merely shipped beside it: all three artifacts are
named from the one `exported_feature` mint, so `feature_id` is the join key across the
bundle. The taxonomy reduction is shared with the shard planner, which must not tile a
genome under one lineage while the client publishes another; the tree's shear is
miint's `shear_tree`, which is also the answer to why `reference_phylogeny` has no
exclusion-aware `_visible` view (see *Phylogeny and Placements*).

Two consequences worth stating here, because neither is visible from any single site:

- **Narrowing the cohort changes the table rather than filtering it.** Breadth of
  coverage is measured over the cohort, so the cohort is part of the scientific
  question — which is also why the ticket mint is all-or-nothing rather than narrowing
  to what the caller may read.
- **Every check in the recipe exists because its failure mode is a table that looks
  right**, not one that errors. The SQL that would publish such a table is unreachable
  without the check having run: the builders take a clearance object only the check
  produces, so "diagnose before you act" is a type constraint rather than a convention.
- **A build writes a bundle, not a file, and the bundle is all-or-nothing.** Its three
  fixed members are the table, the identifier map, and the manifest; `--taxonomy` and
  `--tree` add a fourth and fifth. A failure part-way unlinks every member, including
  ones already committed (`_common.commit_partials`), because a bundle missing the map
  is a table nobody can read and a bundle missing the manifest is one nobody can
  reproduce — coverage filtering makes the table a function of the whole cohort, not
  only of the samples in it, which is why the manifest is a member rather than a flag.

Client half: `qiita_control_plane.cli.user.feature_table`, with `cli.user.alignment` for
the discovery reads that yield an `alignment_idx`. The reference is read out of the
alignment's own `params` rather than taken as a flag, so a caller cannot fetch the
genome map for a different reference than the alignment used — and those `params` are
verified against the digest the server reported for them (`_alignment_summary`) before
anything is read off them, because the manifest cites that digest as the processing's
reproducibility key.
