package eu.dissco.core.digitalspecimenprocessor.domain;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    DigitalSpecimen digitalSpecimen) {

}
