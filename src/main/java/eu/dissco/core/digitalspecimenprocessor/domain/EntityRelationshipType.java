package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

public enum EntityRelationshipType {
  HAS_MEDIA("hasDigitalMedia"),
  HAS_SPECIMEN("hasDigitalSpecimen");

  @Getter
  final String relationshipName;

  EntityRelationshipType(String relationshipName) {
    this.relationshipName = relationshipName;
  }
}
