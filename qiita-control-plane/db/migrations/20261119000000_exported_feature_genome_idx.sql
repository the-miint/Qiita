-- migrate:up

-- Every deleted genome fires exported_feature.genome_idx's ON DELETE SET NULL, which
-- finds the rows by `genome_idx = $1`. The partial exported_feature_live_genome index
-- (NOT retired AND genome_idx IS NOT NULL) cannot serve that lookup, so without this
-- index each deleted genome scans the table. Measured on Postgres 17 with the table
-- holding 93,242 rows, all genome labels: deleting 196,062 genomes took 8 min 46 s
-- without it, 3.3 s with it, and building it took 19.8 ms.
CREATE INDEX exported_feature_genome_idx ON qiita.exported_feature (genome_idx);

-- migrate:down

DROP INDEX qiita.exported_feature_genome_idx;
