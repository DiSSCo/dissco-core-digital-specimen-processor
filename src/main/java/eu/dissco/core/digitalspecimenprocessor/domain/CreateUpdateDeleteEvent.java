package eu.dissco.core.digitalspecimenprocessor.domain;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.UUID;

public record CreateUpdateDeleteEvent(
    UUID id,
    String eventType,
    String agent,
    String subject,
    String subjectType,
    Instant timestamp,
    JsonNode eventRecord,
    JsonNode change,
    String comment) {

}
