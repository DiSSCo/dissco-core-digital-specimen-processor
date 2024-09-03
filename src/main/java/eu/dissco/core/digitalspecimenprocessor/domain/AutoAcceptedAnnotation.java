package eu.dissco.core.digitalspecimenprocessor.domain;

import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;

public record AutoAcceptedAnnotation(
    Agent acceptingAgent,
    AnnotationProcessingRequest annotation
) {

}
