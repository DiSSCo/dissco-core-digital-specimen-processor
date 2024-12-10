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
  MEDIA_HOST("mediaHost"),
  MEDIA_HOST_NAME("mediaHostName"),
  LINKED_DO_PID("linkedDigitalObjectPid"),
  LINKED_DO_TYPE("linkedDigitalObjectType"),
  PRIMARY_MEDIA_ID("primaryMediaId"),
  PRIMARY_MEDIA_ID_TYPE("primaryMediaIdType"),
  PRIMARY_MEDIA_ID_NAME("primaryMediaIdName"),
  MEDIA_TYPE("mediaType"),
  MIME_TYPE("mimeType"),
  LICENSE_NAME("licenseName"),
  LICENSE_URL("licenseUrl"),
  RIGHTS_HOLDER_PID("rightsHolderPid"),
  RIGHTS_HOLDER("rightsHolder"),
  DIGITAL_MEDIA_KEY("digitalMediaKey");

  private final String attribute;

  FdoProfileAttributes(String attribute) {
    this.attribute = attribute;
  }

}
