package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

public enum FdoType {
  SPECIMEN("ods:DigitalSpecimen", "https://doi.org/21.T11148/894b1e6cad57e921764e"),
  MEDIA("ods:DigitalMedia", "https://doi.org/21.T11148/bbad8c4e101e8af01115");

  @Getter
  final String pid;
  @Getter
  final String type;

  FdoType(String type, String pid) {
    this.type = type;
    this.pid = pid;
  }
}
