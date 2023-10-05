package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record DigitalMediaObjectEvent(
    List<String> enrichmentList,
    DigitalMediaObjectWrapper digitalMediaObjectWrapper) {

}
