package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

public enum EntityRelationshipType {
  HAS_MEDIA("hasDigitalMedia"),
  HAS_SPECIMEN("hasDigitalSpecimen");

  @Getter
  String name;

  EntityRelationshipType(String s) {
    this.name = s;
  }
}
