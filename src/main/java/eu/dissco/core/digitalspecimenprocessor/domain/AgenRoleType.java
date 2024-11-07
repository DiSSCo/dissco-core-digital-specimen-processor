package eu.dissco.core.digitalspecimenprocessor.domain;

public enum AgenRoleType {

  COLLECTOR("collector"),
  DATA_TRANSLATOR("data-translator"),
  CREATOR("creator"),
  IDENTIFIER("identifier"),
  GEOREFERENCER("georeferencer"),
  RIGHTS_OWNER("rights-owner"),
  PROCESSING_SERVICE("processing-service"),
  SOURCE_SYSTEM("source-system");

  private final String name;

  AgenRoleType(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
