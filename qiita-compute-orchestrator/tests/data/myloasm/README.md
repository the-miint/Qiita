# myloasm output fixtures

Copied unchanged from duckdb-miint's `data/myloasm/` at commit `e88619bf` (the build the
DuckDB 2.0 mirror serves). duckdb-miint's `data/myloasm/README.md` says how the reads were
built and how to regenerate the files.

- `ecoli_sim/`: a real myloasm **0.7.0** output directory, holding only the files the
  `read_myloasm_*` readers open. Five contigs (three primary, one alternate, one
  duplicated), 1085 read mappings, 40 chimera breakpoints, and two circular self-links.
- `ecoli_sim_0.6.0/`: the same reads assembled by myloasm **0.6.0**, the version production
  pins. Only the two files that differ are kept: the GFA, whose `L` lines carry no tags,
  and the log. Overlay them on `ecoli_sim/` to get a 0.6.0 directory.

Text files are stored gzipped with `.gz` added. myloasm itself gzips
`3-mapping/map_to_unitigs.paf.gz` and both `read_coverages.txt.gz`, so those keep their
names. `tests/helpers.py::myloasm_output_dir` lays a directory out the way myloasm writes it.
