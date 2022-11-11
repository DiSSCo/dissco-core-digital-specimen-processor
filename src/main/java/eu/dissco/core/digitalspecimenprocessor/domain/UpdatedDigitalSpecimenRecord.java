package eu.dissco.core.digitalspecimenprocessor.domain;

import java.util.List;

public record UpdatedDigitalSpecimenRecord(DigitalSpecimenRecord digitalSpecimenRecord,
                                           List<String> enrichment,
                                           DigitalSpecimenRecord currentDigitalSpecimen) {

}
