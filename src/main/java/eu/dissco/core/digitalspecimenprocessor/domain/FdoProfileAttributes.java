package eu.dissco.core.digitalspecimenprocessor.domain;

public enum FdoProfileAttributes {

  // Handle: 1-19
  FDO_PROFILE("fdoProfile", 1),
  FDO_RECORD_LICENSE("fdoRecordLicense", 2), 
  DIGITAL_OBJECT_TYPE("digitalObjectType", 3),
  DIGITAL_OBJECT_NAME("digitalObjectName",4),
  PID("pid", 5),
  PID_ISSUER("pidIssuer", 6),
  PID_ISSUER_NAME("pidIssuerName", 7),
  ISSUED_FOR_AGENT ("issuedForAgent", 8),
  ISSUED_FOR_AGENT_NAME("issuedForAgentName", 9), 
  PID_RECORD_ISSUE_DATE("pidRecordIssueDate", 10),
  PID_RECORD_ISSUE_NUMBER("pidRecordIssueNumber", 11),
  STRUCTURAL_TYPE("structuralType", 12),
  PID_STATUS("pidStatus", 13),

  // Tombstone: 30-39
  TOMBSTONE_TEXT("tombstoneText", 30),
  TOMBSTONE_PIDS("tombstonePids", 31),

  // Doi: 40-49
  REFERENT_TYPE("referentType", 40),
  REFERENT_DOI_NAME("referentDoiName", 41),
  REFERENT_NAME("referentName", 42),
  PRIMARY_REFERENT_TYPE("primaryReferentType", 43),
  REFERENT("referent", 44),

  //Digital Specimen: 200-299
  SPECIMEN_HOST("specimenHost", 200),
  SPECIMEN_HOST_NAME("specimenHostName", 201),
  PRIMARY_SPECIMEN_OBJECT_ID("primarySpecimenObjectId", 202),
  PRIMARY_SPECIMEN_OBJECT_ID_TYPE("primarySpecimenObjectIdType", 203),
  PRIMARY_SPECIMEN_OBJECT_ID_NAME("primarySpecimenObjectIdName", 204),
  OTHER_SPECIMEN_IDS("otherSpecimenIds", 206),
  TOPIC_ORIGIN("topicOrigin", 207),
  TOPIC_DOMAIN("topicDomain", 208),
  TOPIC_DISCIPLINE("topicDiscipline", 209),
  OBJECT_TYPE("objectType", 210),
  LIVING_OR_PRESERVED("livingOrPreserved", 211),
  BASE_TYPE_OF_SPECIMEN("baseTypeOfSpecimen", 212),
  INFORMATION_ARTEFACT_TYPE("informationArtefactType", 213),
  MATERIAL_SAMPLE_TYPE("materialSampleType", 214),
  MATERIAL_OR_DIGITAL_ENTITY("materialOrDigitalEntity", 215),
  MARKED_AS_TYPE("markedAsType", 216),
  WAS_DERIVED_FROM("wasDerivedFrom", 217),

  // Handle Admin: 100-199
  HS_ADMIN("HS_ADMIN", 100),
  LOC("10320/loc", 101);
  
  private final String attribute;
  private final int index;

  public int getIndex(){
    return this.index;
  }

  public String getAttribute(){
    return this.attribute;
  }

  private FdoProfileAttributes(String attribute, int index){
    this.attribute = attribute;
    this.index = index;
  }

}
