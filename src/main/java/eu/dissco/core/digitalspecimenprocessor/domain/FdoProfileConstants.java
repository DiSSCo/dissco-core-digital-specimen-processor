package eu.dissco.core.digitalspecimenprocessor.domain;

public enum FdoProfileConstants {
  DIGITAL_SPECIMEN_TYPE("digitalSpecimen"),
  FDO_PROFILE("https://hdl.handle.net/21.T11148/d8de0819e144e4096645"),
  DIGITAL_OBJECT_TYPE("https://hdl.handle.net/21.T11148/894b1e6cad57e921764e"),
  ISSUED_FOR_AGENT_PID("https://ror.org/0566bfb96");
  private final String value;

  FdoProfileConstants(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }
}
