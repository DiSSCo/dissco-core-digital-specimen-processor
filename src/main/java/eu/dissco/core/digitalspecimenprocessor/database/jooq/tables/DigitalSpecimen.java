/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq.tables;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.Indexes;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.Keys;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.Public;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records.DigitalSpecimenRecord;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Index;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.PlainSQL;
import org.jooq.QueryPart;
import org.jooq.SQL;
import org.jooq.Schema;
import org.jooq.Select;
import org.jooq.Stringly;
import org.jooq.Table;
import org.jooq.TableField;
import org.jooq.TableOptions;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.impl.TableImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DigitalSpecimen extends TableImpl<DigitalSpecimenRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.digital_specimen</code>
     */
    public static final DigitalSpecimen DIGITAL_SPECIMEN = new DigitalSpecimen();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<DigitalSpecimenRecord> getRecordType() {
        return DigitalSpecimenRecord.class;
    }

    /**
     * The column <code>public.digital_specimen.id</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> ID = createField(DSL.name("id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.version</code>.
     */
    public final TableField<DigitalSpecimenRecord, Integer> VERSION = createField(DSL.name("version"), SQLDataType.INTEGER.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.type</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> TYPE = createField(DSL.name("type"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.midslevel</code>.
     */
    public final TableField<DigitalSpecimenRecord, Short> MIDSLEVEL = createField(DSL.name("midslevel"), SQLDataType.SMALLINT.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.physical_specimen_id</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> PHYSICAL_SPECIMEN_ID = createField(DSL.name("physical_specimen_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.physical_specimen_type</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> PHYSICAL_SPECIMEN_TYPE = createField(DSL.name("physical_specimen_type"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.specimen_name</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> SPECIMEN_NAME = createField(DSL.name("specimen_name"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.digital_specimen.organization_id</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> ORGANIZATION_ID = createField(DSL.name("organization_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.source_system_id</code>.
     */
    public final TableField<DigitalSpecimenRecord, String> SOURCE_SYSTEM_ID = createField(DSL.name("source_system_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.created</code>.
     */
    public final TableField<DigitalSpecimenRecord, Instant> CREATED = createField(DSL.name("created"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.last_checked</code>.
     */
    public final TableField<DigitalSpecimenRecord, Instant> LAST_CHECKED = createField(DSL.name("last_checked"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.digital_specimen.deleted</code>.
     */
    public final TableField<DigitalSpecimenRecord, Instant> DELETED = createField(DSL.name("deleted"), SQLDataType.INSTANT, this, "");

    /**
     * The column <code>public.digital_specimen.data</code>.
     */
    public final TableField<DigitalSpecimenRecord, JSONB> DATA = createField(DSL.name("data"), SQLDataType.JSONB, this, "");

    /**
     * The column <code>public.digital_specimen.original_data</code>.
     */
    public final TableField<DigitalSpecimenRecord, JSONB> ORIGINAL_DATA = createField(DSL.name("original_data"), SQLDataType.JSONB, this, "");

    private DigitalSpecimen(Name alias, Table<DigitalSpecimenRecord> aliased) {
        this(alias, aliased, (Field<?>[]) null, null);
    }

    private DigitalSpecimen(Name alias, Table<DigitalSpecimenRecord> aliased, Field<?>[] parameters, Condition where) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table(), where);
    }

    /**
     * Create an aliased <code>public.digital_specimen</code> table reference
     */
    public DigitalSpecimen(String alias) {
        this(DSL.name(alias), DIGITAL_SPECIMEN);
    }

    /**
     * Create an aliased <code>public.digital_specimen</code> table reference
     */
    public DigitalSpecimen(Name alias) {
        this(alias, DIGITAL_SPECIMEN);
    }

    /**
     * Create a <code>public.digital_specimen</code> table reference
     */
    public DigitalSpecimen() {
        this(DSL.name("digital_specimen"), null);
    }

    @Override
    public Schema getSchema() {
        return aliased() ? null : Public.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.asList(Indexes.DIGITAL_SPECIMEN_CREATED_IDX, Indexes.DIGITAL_SPECIMEN_PHYSICAL_SPECIMEN_ID_IDX);
    }

    @Override
    public UniqueKey<DigitalSpecimenRecord> getPrimaryKey() {
        return Keys.DIGITAL_SPECIMEN_PK;
    }

    @Override
    public DigitalSpecimen as(String alias) {
        return new DigitalSpecimen(DSL.name(alias), this);
    }

    @Override
    public DigitalSpecimen as(Name alias) {
        return new DigitalSpecimen(alias, this);
    }

    @Override
    public DigitalSpecimen as(Table<?> alias) {
        return new DigitalSpecimen(alias.getQualifiedName(), this);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalSpecimen rename(String name) {
        return new DigitalSpecimen(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalSpecimen rename(Name name) {
        return new DigitalSpecimen(name, null);
    }

    /**
     * Rename this table
     */
    @Override
    public DigitalSpecimen rename(Table<?> name) {
        return new DigitalSpecimen(name.getQualifiedName(), null);
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen where(Condition condition) {
        return new DigitalSpecimen(getQualifiedName(), aliased() ? this : null, null, condition);
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen where(Collection<? extends Condition> conditions) {
        return where(DSL.and(conditions));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen where(Condition... conditions) {
        return where(DSL.and(conditions));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen where(Field<Boolean> condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public DigitalSpecimen where(SQL condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public DigitalSpecimen where(@Stringly.SQL String condition) {
        return where(DSL.condition(condition));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public DigitalSpecimen where(@Stringly.SQL String condition, Object... binds) {
        return where(DSL.condition(condition, binds));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    @PlainSQL
    public DigitalSpecimen where(@Stringly.SQL String condition, QueryPart... parts) {
        return where(DSL.condition(condition, parts));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen whereExists(Select<?> select) {
        return where(DSL.exists(select));
    }

    /**
     * Create an inline derived table from this table
     */
    @Override
    public DigitalSpecimen whereNotExists(Select<?> select) {
        return where(DSL.notExists(select));
    }
}
