package eu.dissco.core.digitalspecimenprocessor.domain.mas;

import java.util.List;

public record SourceSystemMass(
    List<String> specimenMass,
    List<String> mediaMass
) {

}
