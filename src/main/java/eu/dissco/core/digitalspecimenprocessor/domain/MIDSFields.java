package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public enum MIDSFields {

  // MIDS_0
  PHYSICAL_SPECIMEN_ID("ods:physicalSpecimenId", 0),
  ORGANISATION_ID("ods:organizationId", 0),

  // MIDS_1
  LICENSE("dcterms:license", 1),
  MODIFIED("ods:modified", 1),
  OBJECT_TYPE("ods:objectType", 1),
  SPECIMEN_TYPE("ods:type", 1),
  NAME("ods:specimenName", 1);

  public static final List<MIDSFields> MIDS_0 = List.of(PHYSICAL_SPECIMEN_ID, ORGANISATION_ID);
  public static final List<MIDSFields> MIDS_1 = List.of(LICENSE, MODIFIED, OBJECT_TYPE,
      SPECIMEN_TYPE, NAME);

  private final String term;
  private final int midsLevel;

  MIDSFields(String term, int midsLevel) {
    this.term = term;
    this.midsLevel = midsLevel;
  }


  public String getTerm() {
    return term;
  }

  public int getMidsLevel() {
    return midsLevel;
  }

}
