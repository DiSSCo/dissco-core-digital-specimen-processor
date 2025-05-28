package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

public enum FdoType {
  SPECIMEN("https://doi.org/21.T11148/894b1e6cad57e921764e"),
  MEDIA("https://doi.org/21.T11148/bbad8c4e101e8af01115");

  @Getter
  final String pid;

  FdoType(String pid) {
    this.pid = pid;
  }
}
