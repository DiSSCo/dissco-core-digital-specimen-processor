package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import java.time.Instant;
import java.util.Set;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    Instant created,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<String> masIds,
    Boolean forceMasSchedule) {

}
