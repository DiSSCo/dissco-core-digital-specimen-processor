/*
 * This file is generated by jOOQ.
 */
package eu.dissco.core.digitalspecimenprocessor.database.jooq;


import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.Handles;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.NewDigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records.HandlesRecord;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.tables.records.NewDigitalSpecimenRecord;

import org.jooq.TableField;
import org.jooq.UniqueKey;
import org.jooq.impl.DSL;
import org.jooq.impl.Internal;


/**
 * A class modelling foreign key relationships and constraints of tables in 
 * public.
 */
@SuppressWarnings({ "all", "unchecked", "rawtypes" })
public class Keys {

    // -------------------------------------------------------------------------
    // UNIQUE and PRIMARY KEY definitions
    // -------------------------------------------------------------------------

    public static final UniqueKey<HandlesRecord> HANDLES_PKEY = Internal.createUniqueKey(Handles.HANDLES, DSL.name("handles_pkey"), new TableField[] { Handles.HANDLES.HANDLE, Handles.HANDLES.IDX }, true);
    public static final UniqueKey<NewDigitalSpecimenRecord> NEW_DIGITAL_SPECIMEN_PHYSICAL_SPECIMEN_ID_KEY = Internal.createUniqueKey(NewDigitalSpecimen.NEW_DIGITAL_SPECIMEN, DSL.name("new_digital_specimen_physical_specimen_id_key"), new TableField[] { NewDigitalSpecimen.NEW_DIGITAL_SPECIMEN.PHYSICAL_SPECIMEN_ID }, true);
    public static final UniqueKey<NewDigitalSpecimenRecord> NEW_DIGITAL_SPECIMEN_PKEY = Internal.createUniqueKey(NewDigitalSpecimen.NEW_DIGITAL_SPECIMEN, DSL.name("new_digital_specimen_pkey"), new TableField[] { NewDigitalSpecimen.NEW_DIGITAL_SPECIMEN.ID, NewDigitalSpecimen.NEW_DIGITAL_SPECIMEN.VERSION }, true);
}
