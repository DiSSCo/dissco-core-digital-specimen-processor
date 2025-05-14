create table digital_specimen
(
    id                     text                     not null
        constraint digital_specimen_pk
            primary key,
    version                integer                  not null,
    type                   text                     not null,
    midslevel              smallint                 not null,
    physical_specimen_id   text                     not null,
    physical_specimen_type text                     not null,
    specimen_name          text,
    organization_id        text                     not null,
    source_system_id       text                     not null,
    created                timestamp with time zone not null,
    last_checked           timestamp with time zone not null,
    deleted                timestamp with time zone,
    data                   jsonb,
    original_data          jsonb,
    modified               timestamp with time zone
);

create index digital_specimen_created_idx
    on digital_specimen (created);

create index digital_specimen_physical_specimen_id_idx
    on digital_specimen (physical_specimen_id);

create type translator_type as enum ('biocase', 'dwca');

create table digital_media_object
(
    id                  text                     not null
        constraint digital_media_object_pk
            primary key,
    version             integer                  not null,
    type                text,
    digital_specimen_id text                     ,
    media_url           text                     not null,
    created             timestamp with time zone not null,
    last_checked        timestamp with time zone not null,
    deleted             timestamp with time zone,
    data                jsonb                    not null,
    original_data       jsonb                    not null,
    modified            timestamp with time zone
);