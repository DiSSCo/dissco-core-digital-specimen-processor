package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import java.util.List;

public record PidProcessResult(
    String doi,
    List<String> relatedDois
){


}
