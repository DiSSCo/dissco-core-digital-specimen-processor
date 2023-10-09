create table digital_specimen
(
    id text not null
        constraint digital_specimen_pk
            primary key,
    version integer not null,
    type text not null,
    midslevel smallint not null,
    physical_specimen_id text not null,
    physical_specimen_type text not null,
    specimen_name text,
    organization_id text not null,
    source_system_id text not null,
    created timestamp with time zone not null,
    last_checked timestamp with time zone not null,
    deleted timestamp with time zone,
    data jsonb,
    original_data jsonb
);

create index digital_specimen_created_idx
    on digital_specimen (created);

create index digital_specimen_physical_specimen_id_idx
    on digital_specimen (physical_specimen_id);
