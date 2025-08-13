package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import java.util.List;
import java.util.Set;

public record DigitalSpecimenEvent(
    Set<String> masList,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    List<DigitalMediaEvent> digitalMediaEvents,
    Boolean forceMasSchedule) {

}
