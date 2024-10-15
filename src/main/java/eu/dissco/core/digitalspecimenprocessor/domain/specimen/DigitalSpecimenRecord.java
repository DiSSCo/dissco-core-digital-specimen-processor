package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import java.time.Instant;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    Instant created,
    DigitalSpecimenWrapper digitalSpecimenWrapper) {

}
