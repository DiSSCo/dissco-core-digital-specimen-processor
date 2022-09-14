/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq.tables;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.Keys;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.Public;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records.NewDigitalSpecimenRecord;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.JSONB;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row17;
import org.jooq.Schema;
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
public class NewDigitalSpecimen extends TableImpl<NewDigitalSpecimenRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.new_digital_specimen</code>
     */
    public static final NewDigitalSpecimen NEW_DIGITAL_SPECIMEN = new NewDigitalSpecimen();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<NewDigitalSpecimenRecord> getRecordType() {
        return NewDigitalSpecimenRecord.class;
    }

    /**
     * The column <code>public.new_digital_specimen.id</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> ID = createField(DSL.name("id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.version</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, Integer> VERSION = createField(DSL.name("version"), SQLDataType.INTEGER.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.type</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> TYPE = createField(DSL.name("type"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.midslevel</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, Short> MIDSLEVEL = createField(DSL.name("midslevel"), SQLDataType.SMALLINT.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.physical_specimen_id</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> PHYSICAL_SPECIMEN_ID = createField(DSL.name("physical_specimen_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.physical_specimen_type</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> PHYSICAL_SPECIMEN_TYPE = createField(DSL.name("physical_specimen_type"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.specimen_name</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> SPECIMEN_NAME = createField(DSL.name("specimen_name"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.new_digital_specimen.organization_id</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> ORGANIZATION_ID = createField(DSL.name("organization_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.physical_specimen_collection</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> PHYSICAL_SPECIMEN_COLLECTION = createField(DSL.name("physical_specimen_collection"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.new_digital_specimen.dataset</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> DATASET = createField(DSL.name("dataset"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.new_digital_specimen.source_system_id</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> SOURCE_SYSTEM_ID = createField(DSL.name("source_system_id"), SQLDataType.CLOB.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.created</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, Instant> CREATED = createField(DSL.name("created"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.last_checked</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, Instant> LAST_CHECKED = createField(DSL.name("last_checked"), SQLDataType.INSTANT.nullable(false), this, "");

    /**
     * The column <code>public.new_digital_specimen.deleted</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, Instant> DELETED = createField(DSL.name("deleted"), SQLDataType.INSTANT, this, "");

    /**
     * The column <code>public.new_digital_specimen.data</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, JSONB> DATA = createField(DSL.name("data"), SQLDataType.JSONB, this, "");

    /**
     * The column <code>public.new_digital_specimen.original_data</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, JSONB> ORIGINAL_DATA = createField(DSL.name("original_data"), SQLDataType.JSONB, this, "");

    /**
     * The column <code>public.new_digital_specimen.dwca_id</code>.
     */
    public final TableField<NewDigitalSpecimenRecord, String> DWCA_ID = createField(DSL.name("dwca_id"), SQLDataType.CLOB, this, "");

    private NewDigitalSpecimen(Name alias, Table<NewDigitalSpecimenRecord> aliased) {
        this(alias, aliased, null);
    }

    private NewDigitalSpecimen(Name alias, Table<NewDigitalSpecimenRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.new_digital_specimen</code> table reference
     */
    public NewDigitalSpecimen(String alias) {
        this(DSL.name(alias), NEW_DIGITAL_SPECIMEN);
    }

    /**
     * Create an aliased <code>public.new_digital_specimen</code> table reference
     */
    public NewDigitalSpecimen(Name alias) {
        this(alias, NEW_DIGITAL_SPECIMEN);
    }

    /**
     * Create a <code>public.new_digital_specimen</code> table reference
     */
    public NewDigitalSpecimen() {
        this(DSL.name("new_digital_specimen"), null);
    }

    public <O extends Record> NewDigitalSpecimen(Table<O> child, ForeignKey<O, NewDigitalSpecimenRecord> key) {
        super(child, key, NEW_DIGITAL_SPECIMEN);
    }

    @Override
    public Schema getSchema() {
        return Public.PUBLIC;
    }

    @Override
    public UniqueKey<NewDigitalSpecimenRecord> getPrimaryKey() {
        return Keys.NEW_DIGITAL_SPECIMEN_PKEY;
    }

    @Override
    public List<UniqueKey<NewDigitalSpecimenRecord>> getKeys() {
        return Arrays.<UniqueKey<NewDigitalSpecimenRecord>>asList(Keys.NEW_DIGITAL_SPECIMEN_PKEY, Keys.NEW_DIGITAL_SPECIMEN_PHYSICAL_SPECIMEN_ID_KEY);
    }

    @Override
    public NewDigitalSpecimen as(String alias) {
        return new NewDigitalSpecimen(DSL.name(alias), this);
    }

    @Override
    public NewDigitalSpecimen as(Name alias) {
        return new NewDigitalSpecimen(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public NewDigitalSpecimen rename(String name) {
        return new NewDigitalSpecimen(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public NewDigitalSpecimen rename(Name name) {
        return new NewDigitalSpecimen(name, null);
    }

    // -------------------------------------------------------------------------
    // Row17 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row17<String, Integer, String, Short, String, String, String, String, String, String, String, Instant, Instant, Instant, JSONB, JSONB, String> fieldsRow() {
        return (Row17) super.fieldsRow();
    }
}
