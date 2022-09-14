/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq.tables;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.Indexes;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.Keys;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.Public;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records.HandlesRecord;

import java.util.Arrays;
import java.util.List;

import org.jooq.Field;
import org.jooq.ForeignKey;
import org.jooq.Index;
import org.jooq.Name;
import org.jooq.Record;
import org.jooq.Row12;
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
public class Handles extends TableImpl<HandlesRecord> {

    private static final long serialVersionUID = 1L;

    /**
     * The reference instance of <code>public.handles</code>
     */
    public static final Handles HANDLES = new Handles();

    /**
     * The class holding records for this type
     */
    @Override
    public Class<HandlesRecord> getRecordType() {
        return HandlesRecord.class;
    }

    /**
     * The column <code>public.handles.handle</code>.
     */
    public final TableField<HandlesRecord, byte[]> HANDLE = createField(DSL.name("handle"), SQLDataType.BLOB.nullable(false), this, "");

    /**
     * The column <code>public.handles.idx</code>.
     */
    public final TableField<HandlesRecord, Integer> IDX = createField(DSL.name("idx"), SQLDataType.INTEGER.nullable(false), this, "");

    /**
     * The column <code>public.handles.type</code>.
     */
    public final TableField<HandlesRecord, byte[]> TYPE = createField(DSL.name("type"), SQLDataType.BLOB, this, "");

    /**
     * The column <code>public.handles.data</code>.
     */
    public final TableField<HandlesRecord, byte[]> DATA = createField(DSL.name("data"), SQLDataType.BLOB, this, "");

    /**
     * The column <code>public.handles.ttl_type</code>.
     */
    public final TableField<HandlesRecord, Short> TTL_TYPE = createField(DSL.name("ttl_type"), SQLDataType.SMALLINT, this, "");

    /**
     * The column <code>public.handles.ttl</code>.
     */
    public final TableField<HandlesRecord, Integer> TTL = createField(DSL.name("ttl"), SQLDataType.INTEGER, this, "");

    /**
     * The column <code>public.handles.timestamp</code>.
     */
    public final TableField<HandlesRecord, Long> TIMESTAMP = createField(DSL.name("timestamp"), SQLDataType.BIGINT, this, "");

    /**
     * The column <code>public.handles.refs</code>.
     */
    public final TableField<HandlesRecord, String> REFS = createField(DSL.name("refs"), SQLDataType.CLOB, this, "");

    /**
     * The column <code>public.handles.admin_read</code>.
     */
    public final TableField<HandlesRecord, Boolean> ADMIN_READ = createField(DSL.name("admin_read"), SQLDataType.BOOLEAN, this, "");

    /**
     * The column <code>public.handles.admin_write</code>.
     */
    public final TableField<HandlesRecord, Boolean> ADMIN_WRITE = createField(DSL.name("admin_write"), SQLDataType.BOOLEAN, this, "");

    /**
     * The column <code>public.handles.pub_read</code>.
     */
    public final TableField<HandlesRecord, Boolean> PUB_READ = createField(DSL.name("pub_read"), SQLDataType.BOOLEAN, this, "");

    /**
     * The column <code>public.handles.pub_write</code>.
     */
    public final TableField<HandlesRecord, Boolean> PUB_WRITE = createField(DSL.name("pub_write"), SQLDataType.BOOLEAN, this, "");

    private Handles(Name alias, Table<HandlesRecord> aliased) {
        this(alias, aliased, null);
    }

    private Handles(Name alias, Table<HandlesRecord> aliased, Field<?>[] parameters) {
        super(alias, null, aliased, parameters, DSL.comment(""), TableOptions.table());
    }

    /**
     * Create an aliased <code>public.handles</code> table reference
     */
    public Handles(String alias) {
        this(DSL.name(alias), HANDLES);
    }

    /**
     * Create an aliased <code>public.handles</code> table reference
     */
    public Handles(Name alias) {
        this(alias, HANDLES);
    }

    /**
     * Create a <code>public.handles</code> table reference
     */
    public Handles() {
        this(DSL.name("handles"), null);
    }

    public <O extends Record> Handles(Table<O> child, ForeignKey<O, HandlesRecord> key) {
        super(child, key, HANDLES);
    }

    @Override
    public Schema getSchema() {
        return Public.PUBLIC;
    }

    @Override
    public List<Index> getIndexes() {
        return Arrays.<Index>asList(Indexes.DATAINDEX, Indexes.HANDLEINDEX);
    }

    @Override
    public UniqueKey<HandlesRecord> getPrimaryKey() {
        return Keys.HANDLES_PKEY;
    }

    @Override
    public List<UniqueKey<HandlesRecord>> getKeys() {
        return Arrays.<UniqueKey<HandlesRecord>>asList(Keys.HANDLES_PKEY);
    }

    @Override
    public Handles as(String alias) {
        return new Handles(DSL.name(alias), this);
    }

    @Override
    public Handles as(Name alias) {
        return new Handles(alias, this);
    }

    /**
     * Rename this table
     */
    @Override
    public Handles rename(String name) {
        return new Handles(DSL.name(name), null);
    }

    /**
     * Rename this table
     */
    @Override
    public Handles rename(Name name) {
        return new Handles(name, null);
    }

    // -------------------------------------------------------------------------
    // Row12 type methods
    // -------------------------------------------------------------------------

    @Override
    public Row12<byte[], Integer, byte[], byte[], Short, Integer, Long, String, Boolean, Boolean, Boolean, Boolean> fieldsRow() {
        return (Row12) super.fieldsRow();
    }
}
