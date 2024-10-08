package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

@Getter
public enum FdoProfileAttributes {

  // Digital Specimen
  REFERENT_NAME("referentName"),
  SPECIMEN_HOST("specimenHost"),
  SPECIMEN_HOST_NAME("specimenHostName"),
  NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID("normalisedPrimarySpecimenObjectId"),
  OTHER_SPECIMEN_IDS("otherSpecimenIds"),
  TOPIC_ORIGIN("topicOrigin"),
  TOPIC_DOMAIN("topicDomain"),
  TOPIC_DISCIPLINE("topicDiscipline"),
  LIVING_OR_PRESERVED("livingOrPreserved"),
  MARKED_AS_TYPE("markedAsType"),

  // Digital Media
  SOURCE_SYSTEM_ID("sourceSystemId"),
  PRIMARY_MEDIA_ID("primaryMediaId"),
  DIGITAL_MEDIA_KEY("digitalMediaKey");

  private final String attribute;

  FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

}
