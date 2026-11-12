-- migrate:up

-- The field is optional: see the resolver's handling of an absent host_taxon_id.
UPDATE qiita.biosample_global_field SET required = false WHERE internal_name = 'host_taxon_id';

-- migrate:down

UPDATE qiita.biosample_global_field SET required = true WHERE internal_name = 'host_taxon_id';
