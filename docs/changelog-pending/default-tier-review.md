### Changed

- **A study that only its public default lets a caller read now shows them a public view (#feat/default-tier-review).**
  - A caller with no grant on a `public`-default study, who is not its owner and has no
    wet-lab admin role, gets `view: "public"` from `GET /api/v1/study/{study_idx}`. That view
    carries the `export_entity_id`, title, alias, description, abstract, funding, the two
    accessions and `default_tier`. It leaves out `study_idx` and the owner, PI and creator idxs,
    the notes, `extra_metadata`, the submission state and both timestamps, and it has no `ETag`.
  - In `GET /api/v1/study`, that study's row has `record_view: "public"` and neither `study_idx`
    nor `updated_at`, and `q` finds it only by words in the public view's fields.
  - The owner, anyone with a grant at or above the study's `default_tier`, and wet-lab admins
    still read the full record.
- **Studies that an ENA import creates are public (#feat/default-tier-review).**
  - A new import creates its study `public`. A migration makes the studies that earlier imports
    created `public` too.
  - The migration refuses to apply, naming the studies, while any of them holds a biosample with
    no ENA sample accession or a sequenced sample with no ENA run accession.
  - A study that an import only matched by accession keeps its tier.
  - The default tier of every global field, and why each is `public`, is now written down in
    `docs/architecture/data-model.md` ("Default tiers").
