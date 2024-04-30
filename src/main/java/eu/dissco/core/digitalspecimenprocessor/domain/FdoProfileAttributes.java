package eu.dissco.core.digitalspecimenprocessor.domain;

public enum FdoProfileAttributes {

  // Handle: 1-19
  FDO_PROFILE("fdoProfile"),
  ISSUED_FOR_AGENT("issuedForAgent"),
  REFERENT_NAME("referentName"),
  SPECIMEN_HOST("specimenHost"),
  SPECIMEN_HOST_NAME("specimenHostName"),
  PRIMARY_SPECIMEN_OBJECT_ID("primarySpecimenObjectId"),
  PRIMARY_SPECIMEN_OBJECT_ID_TYPE("primarySpecimenObjectIdType"),
  NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID("normalisedPrimarySpecimenObjectId"),
  TOPIC_DISCIPLINE("topicDiscipline"),
  LIVING_OR_PRESERVED("livingOrPreserved"),
  MARKED_AS_TYPE("markedAsType"),
  SOURCE_SYSTEM_ID("sourceSystemId");

  private final String attribute;

  FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

  public String getAttribute() {
    return this.attribute;
  }

}
