package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record DigitalMediaObjectEventWithoutDoi(
    List<String> enrichmentList,
    DigitalMediaObjectWithoutDoi digitalMediaObjectWithoutDoi) {

}
