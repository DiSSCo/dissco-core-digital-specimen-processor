/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.DigitalSpecimen;
import org.jooq.Index;
import org.jooq.OrderField;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;


/**
 * A class modelling indexes of tables in public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Indexes {

    // -------------------------------------------------------------------------
    // INDEX definitions
    // -------------------------------------------------------------------------

    public static final Index DIGITAL_SPECIMEN_CREATED_IDX = Internal.createIndex(DSL.name("digital_specimen_created_idx"), DigitalSpecimen.DIGITAL_SPECIMEN, new OrderField[] { DigitalSpecimen.DIGITAL_SPECIMEN.CREATED }, false);
    public static final Index DIGITAL_SPECIMEN_PHYSICAL_SPECIMEN_ID_IDX = Internal.createIndex(DSL.name("digital_specimen_physical_specimen_id_idx"), DigitalSpecimen.DIGITAL_SPECIMEN, new OrderField[] { DigitalSpecimen.DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID }, false);
}
