package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import java.time.Instant;
import java.util.List;
import java.util.Set;

public record DigitalSpecimenRecord(
    String id,
    int midsLevel,
    int version,
    Instant created,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    Set<String> masIds,
    Boolean forceMasSchedule,
    Boolean isDataFromSourceSystem,
    List<DigitalMediaEvent> digitalMediaEvents) {

}
