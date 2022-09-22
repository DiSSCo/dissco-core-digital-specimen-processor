package eu.dissco.core.digitalspecimenprocessor.domain;

import java.time.Instant;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    Instant created,
    DigitalSpecimen digitalSpecimen) {

}
