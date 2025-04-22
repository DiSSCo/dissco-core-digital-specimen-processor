package eu.dissco.core.digitalspecimenprocessor.domain.relation;

import java.util.List;
import java.util.Map;

public record PidProcessResult(
    String doi,
    List<String> relatedDois
){


}
