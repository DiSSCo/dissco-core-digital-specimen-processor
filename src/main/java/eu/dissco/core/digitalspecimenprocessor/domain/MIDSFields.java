package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public enum MIDSFields {

  // MIDS_0
  PHYSICAL_SPECIMEN_ID(List.of("ods:physicalSpecimenId"), 0),
  ORGANISATION_ID(List.of("ods:organizationId"), 0),

  // MIDS_1
  LICENSE(List.of("dcterms:license"), 1),
  MODIFIED(List.of("dcterms:modified"), 1),
  OBJECT_TYPE(List.of("ods:objectType"), 1),
  SPECIMEN_TYPE(List.of("ods:type"), 1),
  NAME(List.of("ods:specimenName"), 1),

  // MIDS_2
  COLLECTING_NUMBER(List.of("ods:collectingNumber"), 2),
  COLLECTOR(List.of("ods:collector"), 2),
  DATE_COLLECTED(List.of("ods:dateCollected"), 2),
  TYPE_STATUS(List.of("dwc:typeStatus"), 2),
  HAS_MEDIA(List.of("ods:hasMedia"), 2),
  STRATIGRAPHY(List.of(
      "dwc:earliestAgeOrLowestStage", "dwc:earliestEonOrLowestEonothem",
      "dwc:earliestEpochOrLowestSeries", "dwc:earliestEraOrLowestErathem",
      "dwc:earliestPeriodOrLowestSystem", "dwc:latestAgeOrHighestStage",
      "dwc:latestEonOrHighestEonothem", "dwc:latestEpochOrHighestSeries",
      "dwc:latestEraOrHighestErathem", "dwc:latestPeriodOrHighestSystem",
      "dwc:bed", "dwc:formation", "dwc:group", "dwc:member", "dwc:highestBiostratigraphicZone",
      "dwc:lowestBiostratigraphicZone"), 2),
  QUALITATIVE_LOCATION(List.of("dwc:continent", "dwc:country", "dwc:countryCode",
      "dwc:county", "dwc:island", "dwc:islandGroup", "dwc:locality",
      "dwc:municipality", "dwc:stateProvince", "dwc:waterBody"), 2),
  QUANTITATIVE_LOCATION(List.of("dwc:decimalLatitude", "dwc:decimalLongitude"), 2);

  public static final List<MIDSFields> MIDS_0 = List.of(PHYSICAL_SPECIMEN_ID, ORGANISATION_ID);
  public static final List<MIDSFields> MIDS_1 = List.of(LICENSE, MODIFIED, OBJECT_TYPE,
      SPECIMEN_TYPE, NAME);
  public static final List<MIDSFields> MIDS_2_BIO = List.of(COLLECTING_NUMBER, COLLECTOR,
      DATE_COLLECTED, TYPE_STATUS, HAS_MEDIA, QUALITATIVE_LOCATION, QUANTITATIVE_LOCATION);
  public static final List<MIDSFields> MIDS_2_GEO_PALEO = List.of(QUALITATIVE_LOCATION,
      QUANTITATIVE_LOCATION, STRATIGRAPHY, TYPE_STATUS);

  private final List<String> term;
  private final int midsLevel;

  MIDSFields(List<String> term, int midsLevel) {
    this.term = term;
    this.midsLevel = midsLevel;
  }


  public List<String> getTerm() {
    return term;
  }

  public int getMidsLevel() {
    return midsLevel;
  }

}
