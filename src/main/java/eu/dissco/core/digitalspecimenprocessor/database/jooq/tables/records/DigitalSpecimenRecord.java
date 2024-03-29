/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.DigitalSpecimen;

import java.time.Instant;

import org.jooq.Field;
import org.jooq.JSONB;
import org.jooq.Record1;
import org.jooq.Record14;
import org.jooq.Row14;
import org.jooq.impl.UpdatableRecordImpl;


/**
 * This class is generated by jOOQ.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class DigitalSpecimenRecord extends UpdatableRecordImpl<DigitalSpecimenRecord> implements Record14<String, Integer, String, Short, String, String, String, String, String, Instant, Instant, Instant, JSONB, JSONB> {

    private static final long serialVersionUID = 1L;

    /**
     * Setter for <code>public.digital_specimen.id</code>.
     */
    public void setId(String value) {
        set(0, value);
    }

    /**
     * Getter for <code>public.digital_specimen.id</code>.
     */
    public String getId() {
        return (String) get(0);
    }

    /**
     * Setter for <code>public.digital_specimen.version</code>.
     */
    public void setVersion(Integer value) {
        set(1, value);
    }

    /**
     * Getter for <code>public.digital_specimen.version</code>.
     */
    public Integer getVersion() {
        return (Integer) get(1);
    }

    /**
     * Setter for <code>public.digital_specimen.type</code>.
     */
    public void setType(String value) {
        set(2, value);
    }

    /**
     * Getter for <code>public.digital_specimen.type</code>.
     */
    public String getType() {
        return (String) get(2);
    }

    /**
     * Setter for <code>public.digital_specimen.midslevel</code>.
     */
    public void setMidslevel(Short value) {
        set(3, value);
    }

    /**
     * Getter for <code>public.digital_specimen.midslevel</code>.
     */
    public Short getMidslevel() {
        return (Short) get(3);
    }

    /**
     * Setter for <code>public.digital_specimen.physical_specimen_id</code>.
     */
    public void setPhysicalSpecimenId(String value) {
        set(4, value);
    }

    /**
     * Getter for <code>public.digital_specimen.physical_specimen_id</code>.
     */
    public String getPhysicalSpecimenId() {
        return (String) get(4);
    }

    /**
     * Setter for <code>public.digital_specimen.physical_specimen_type</code>.
     */
    public void setPhysicalSpecimenType(String value) {
        set(5, value);
    }

    /**
     * Getter for <code>public.digital_specimen.physical_specimen_type</code>.
     */
    public String getPhysicalSpecimenType() {
        return (String) get(5);
    }

    /**
     * Setter for <code>public.digital_specimen.specimen_name</code>.
     */
    public void setSpecimenName(String value) {
        set(6, value);
    }

    /**
     * Getter for <code>public.digital_specimen.specimen_name</code>.
     */
    public String getSpecimenName() {
        return (String) get(6);
    }

    /**
     * Setter for <code>public.digital_specimen.organization_id</code>.
     */
    public void setOrganizationId(String value) {
        set(7, value);
    }

    /**
     * Getter for <code>public.digital_specimen.organization_id</code>.
     */
    public String getOrganizationId() {
        return (String) get(7);
    }

    /**
     * Setter for <code>public.digital_specimen.source_system_id</code>.
     */
    public void setSourceSystemId(String value) {
        set(8, value);
    }

    /**
     * Getter for <code>public.digital_specimen.source_system_id</code>.
     */
    public String getSourceSystemId() {
        return (String) get(8);
    }

    /**
     * Setter for <code>public.digital_specimen.created</code>.
     */
    public void setCreated(Instant value) {
        set(9, value);
    }

    /**
     * Getter for <code>public.digital_specimen.created</code>.
     */
    public Instant getCreated() {
        return (Instant) get(9);
    }

    /**
     * Setter for <code>public.digital_specimen.last_checked</code>.
     */
    public void setLastChecked(Instant value) {
        set(10, value);
    }

    /**
     * Getter for <code>public.digital_specimen.last_checked</code>.
     */
    public Instant getLastChecked() {
        return (Instant) get(10);
    }

    /**
     * Setter for <code>public.digital_specimen.deleted</code>.
     */
    public void setDeleted(Instant value) {
        set(11, value);
    }

    /**
     * Getter for <code>public.digital_specimen.deleted</code>.
     */
    public Instant getDeleted() {
        return (Instant) get(11);
    }

    /**
     * Setter for <code>public.digital_specimen.data</code>.
     */
    public void setData(JSONB value) {
        set(12, value);
    }

    /**
     * Getter for <code>public.digital_specimen.data</code>.
     */
    public JSONB getData() {
        return (JSONB) get(12);
    }

    /**
     * Setter for <code>public.digital_specimen.original_data</code>.
     */
    public void setOriginalData(JSONB value) {
        set(13, value);
    }

    /**
     * Getter for <code>public.digital_specimen.original_data</code>.
     */
    public JSONB getOriginalData() {
        return (JSONB) get(13);
    }

    // -------------------------------------------------------------------------
    // Primary key information
    // -------------------------------------------------------------------------

    @Override
    public Record1<String> key() {
        return (Record1) super.key();
    }

    // -------------------------------------------------------------------------
    // Record14 type implementation
    // -------------------------------------------------------------------------

    @Override
    public Row14<String, Integer, String, Short, String, String, String, String, String, Instant, Instant, Instant, JSONB, JSONB> fieldsRow() {
        return (Row14) super.fieldsRow();
    }

    @Override
    public Row14<String, Integer, String, Short, String, String, String, String, String, Instant, Instant, Instant, JSONB, JSONB> valuesRow() {
        return (Row14) super.valuesRow();
    }

    @Override
    public Field<String> field1() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.ID;
    }

    @Override
    public Field<Integer> field2() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.VERSION;
    }

    @Override
    public Field<String> field3() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.TYPE;
    }

    @Override
    public Field<Short> field4() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.MIDSLEVEL;
    }

    @Override
    public Field<String> field5() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID;
    }

    @Override
    public Field<String> field6() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_TYPE;
    }

    @Override
    public Field<String> field7() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.SPECIMEN_NAME;
    }

    @Override
    public Field<String> field8() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.ORGANIZATION_ID;
    }

    @Override
    public Field<String> field9() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.SOURCE_SYSTEM_ID;
    }

    @Override
    public Field<Instant> field10() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.CREATED;
    }

    @Override
    public Field<Instant> field11() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.LAST_CHECKED;
    }

    @Override
    public Field<Instant> field12() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.DELETED;
    }

    @Override
    public Field<JSONB> field13() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.DATA;
    }

    @Override
    public Field<JSONB> field14() {
        return DigitalSpecimen.DIGITAL_SPECIMEN.ORIGINAL_DATA;
    }

    @Override
    public String component1() {
        return getId();
    }

    @Override
    public Integer component2() {
        return getVersion();
    }

    @Override
    public String component3() {
        return getType();
    }

    @Override
    public Short component4() {
        return getMidslevel();
    }

    @Override
    public String component5() {
        return getPhysicalSpecimenId();
    }

    @Override
    public String component6() {
        return getPhysicalSpecimenType();
    }

    @Override
    public String component7() {
        return getSpecimenName();
    }

    @Override
    public String component8() {
        return getOrganizationId();
    }

    @Override
    public String component9() {
        return getSourceSystemId();
    }

    @Override
    public Instant component10() {
        return getCreated();
    }

    @Override
    public Instant component11() {
        return getLastChecked();
    }

    @Override
    public Instant component12() {
        return getDeleted();
    }

    @Override
    public JSONB component13() {
        return getData();
    }

    @Override
    public JSONB component14() {
        return getOriginalData();
    }

    @Override
    public String value1() {
        return getId();
    }

    @Override
    public Integer value2() {
        return getVersion();
    }

    @Override
    public String value3() {
        return getType();
    }

    @Override
    public Short value4() {
        return getMidslevel();
    }

    @Override
    public String value5() {
        return getPhysicalSpecimenId();
    }

    @Override
    public String value6() {
        return getPhysicalSpecimenType();
    }

    @Override
    public String value7() {
        return getSpecimenName();
    }

    @Override
    public String value8() {
        return getOrganizationId();
    }

    @Override
    public String value9() {
        return getSourceSystemId();
    }

    @Override
    public Instant value10() {
        return getCreated();
    }

    @Override
    public Instant value11() {
        return getLastChecked();
    }

    @Override
    public Instant value12() {
        return getDeleted();
    }

    @Override
    public JSONB value13() {
        return getData();
    }

    @Override
    public JSONB value14() {
        return getOriginalData();
    }

    @Override
    public DigitalSpecimenRecord value1(String value) {
        setId(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value2(Integer value) {
        setVersion(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value3(String value) {
        setType(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value4(Short value) {
        setMidslevel(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value5(String value) {
        setPhysicalSpecimenId(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value6(String value) {
        setPhysicalSpecimenType(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value7(String value) {
        setSpecimenName(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value8(String value) {
        setOrganizationId(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value9(String value) {
        setSourceSystemId(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value10(Instant value) {
        setCreated(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value11(Instant value) {
        setLastChecked(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value12(Instant value) {
        setDeleted(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value13(JSONB value) {
        setData(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord value14(JSONB value) {
        setOriginalData(value);
        return this;
    }

    @Override
    public DigitalSpecimenRecord values(String value1, Integer value2, String value3, Short value4, String value5, String value6, String value7, String value8, String value9, Instant value10, Instant value11, Instant value12, JSONB value13, JSONB value14) {
        value1(value1);
        value2(value2);
        value3(value3);
        value4(value4);
        value5(value5);
        value6(value6);
        value7(value7);
        value8(value8);
        value9(value9);
        value10(value10);
        value11(value11);
        value12(value12);
        value13(value13);
        value14(value14);
        return this;
    }

    // -------------------------------------------------------------------------
    // Constructors
    // -------------------------------------------------------------------------

    /**
     * Create a detached DigitalSpecimenRecord
     */
    public DigitalSpecimenRecord() {
        super(DigitalSpecimen.DIGITAL_SPECIMEN);
    }

    /**
     * Create a detached, initialised DigitalSpecimenRecord
     */
    public DigitalSpecimenRecord(String id, Integer version, String type, Short midslevel, String physicalSpecimenId, String physicalSpecimenType, String specimenName, String organizationId, String sourceSystemId, Instant created, Instant lastChecked, Instant deleted, JSONB data, JSONB originalData) {
        super(DigitalSpecimen.DIGITAL_SPECIMEN);

        setId(id);
        setVersion(version);
        setType(type);
        setMidslevel(midslevel);
        setPhysicalSpecimenId(physicalSpecimenId);
        setPhysicalSpecimenType(physicalSpecimenType);
        setSpecimenName(specimenName);
        setOrganizationId(organizationId);
        setSourceSystemId(sourceSystemId);
        setCreated(created);
        setLastChecked(lastChecked);
        setDeleted(deleted);
        setData(data);
        setOriginalData(originalData);
        resetChangedOnNotNull();
    }
}
