package eu.dissco.core.digitalspecimenprocessor.domain.specimen;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import java.util.List;

public record DigitalSpecimenEvent(
    List<String> enrichmentList,
    DigitalSpecimenWrapper digitalSpecimenWrapper,
    List<DigitalMediaEvent> digitalMediaEvents) {

}
