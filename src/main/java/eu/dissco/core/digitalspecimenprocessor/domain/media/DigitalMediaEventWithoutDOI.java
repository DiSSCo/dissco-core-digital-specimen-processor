package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record DigitalMediaEventWithoutDOI(
    List<String> enrichmentList,
    @JsonProperty("digitalMedia")
    DigitalMediaWithoutDOI digitalMediaObjectWithoutDoi) {

}
