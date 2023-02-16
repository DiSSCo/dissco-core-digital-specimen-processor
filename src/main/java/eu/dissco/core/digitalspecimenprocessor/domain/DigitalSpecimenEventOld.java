package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record DigitalSpecimenEventOld(
    List<String> enrichmentList,
    DigitalSpecimenOld digitalSpecimen) {

}
