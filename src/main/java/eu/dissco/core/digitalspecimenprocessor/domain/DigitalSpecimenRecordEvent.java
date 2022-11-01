package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record DigitalSpecimenRecordEvent(
    List<String> enrichmentList,
    DigitalSpecimenRecord digitalSpecimenRecord) {

}
