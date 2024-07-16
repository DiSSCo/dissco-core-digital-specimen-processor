package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record DigitalMediaEvent(
    List<String> enrichmentList,
    DigitalMediaWrapper digitalMediaWrapper) {

}
