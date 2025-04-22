package eu.dissco.core.digitalspecimenprocessor.domain.media;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

public record DigitalMediaEvent(
    List<String> enrichmentList,
    @JsonProperty("digitalMedia")
    DigitalMediaWrapper digitalMediaWrapper) {

}
