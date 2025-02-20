package eu.dissco.core.digitalspecimenprocessor.domain;

import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import java.util.List;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    List<AnnotationProcessingRequest> annotations
) {

}
