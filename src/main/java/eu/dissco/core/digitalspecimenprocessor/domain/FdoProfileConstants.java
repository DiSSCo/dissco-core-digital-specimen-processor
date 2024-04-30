package eu.dissco.core.digitalspecimenprocessor.domain;

import lombok.Getter;

@Getter
public enum FdoProfileConstants {
  DIGITAL_SPECIMEN_TYPE("https://hdl.handle.net/21.T11148/894b1e6cad57e921764"),
  FDO_PROFILE("https://hdl.handle.net/21.T11148/894b1e6cad57e921764"),
  ISSUED_FOR_AGENT_PID("https://ror.org/0566bfb96");
  private final String value;

  FdoProfileConstants(String value) {
    this.value = value;
  }

}
