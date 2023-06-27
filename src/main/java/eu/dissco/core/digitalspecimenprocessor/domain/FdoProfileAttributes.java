package eu.dissco.core.digitalspecimenprocessor.domain;

public enum FdoProfileAttributes {

  // Handle: 1-19
  FDO_PROFILE("fdoProfile", 1),
  DIGITAL_OBJECT_TYPE("digitalObjectType", 3),
  ISSUED_FOR_AGENT ("issuedForAgent", 8),
  REFERENT_NAME("referentName", 42),
  SPECIMEN_HOST("specimenHost", 200),
  SPECIMEN_HOST_NAME("specimenHostName", 201),
  PRIMARY_SPECIMEN_OBJECT_ID("primarySpecimenObjectId", 202),
  PRIMARY_SPECIMEN_OBJECT_ID_TYPE("primarySpecimenObjectIdType", 203),
  TOPIC_DISCIPLINE("topicDiscipline", 209),
  LIVING_OR_PRESERVED("livingOrPreserved", 211),
  MARKED_AS_TYPE("markedAsType", 216);

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
