-- migrate:up

-- An absent host_taxon_id means the sample has no host, so the field is optional.
UPDATE qiita.biosample_global_field SET required = false WHERE internal_name = 'host_taxon_id';

-- migrate:down

UPDATE qiita.biosample_global_field SET required = true WHERE internal_name = 'host_taxon_id';
