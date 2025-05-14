package eu.dissco.core.digitalspecimenprocessor.service;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_SPECIMEN;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jsonpatch.diff.JsonDiff;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EqualityService {

  private final Configuration jsonPathConfig;
  private final JsonFactory factory;
  private final ObjectMapper mapper;
  private static final List<String> IGNORED_FIELDS = List.of(
      "dcterms:created",
      "dcterms:modified",
      "dwc:relationshipEstablishedDate"
  );
  private static final String ENTITY_RELATIONSHIPS = "ENTITY_RELATIONSHIPS";

  public boolean specimensAreEqual(DigitalSpecimenRecord currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimenWrapper,
      MediaRelationshipProcessResult mediaRelationshipProcessResult) {
    return specimensAreEqual(currentDigitalSpecimen, digitalSpecimenWrapper)
        && mediaRelationshipProcessResult.newLinkedObjects().isEmpty()
        && mediaRelationshipProcessResult.tombstonedRelationships().isEmpty();
  }

  public boolean mediaAreEqual(DigitalMediaRecord currentDigitalMedia,
      DigitalMediaWrapper digitalMedia,
      Set<String> newSpecimenRelationships) {
    return mediaAreEqual(currentDigitalMedia, digitalMedia)
        && newSpecimenRelationships.isEmpty();
  }

  public DigitalSpecimenEvent setExistingEventDatesSpecimen(
      DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenEvent digitalSpecimenEvent,
      MediaRelationshipProcessResult mediaRelationships) {
    var digitalSpecimen = digitalSpecimenEvent.digitalSpecimenWrapper().attributes();
    setEntityRelationshipDates( // Set dates of ERs to previous version
        currentDigitalSpecimenWrapper.attributes().getOdsHasEntityRelationships(),
        digitalSpecimen.getOdsHasEntityRelationships());
    digitalSpecimen.setOdsHasEntityRelationships(addExistingMediaRelationships(mediaRelationships, // add existing media ers
        digitalSpecimen.getOdsHasEntityRelationships()));
    // Set dcterms:created to original date
    digitalSpecimen.withDctermsCreated(
        currentDigitalSpecimenWrapper.attributes().getDctermsCreated());
    // We create a new object because the events/wrappers are immutable, and we don't want the hash code to be out of sync
    return new DigitalSpecimenEvent(digitalSpecimenEvent.enrichmentList(),
        new DigitalSpecimenWrapper(
            digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID(),
            digitalSpecimenEvent.digitalSpecimenWrapper().type(),
            digitalSpecimen,
            digitalSpecimenEvent.digitalSpecimenWrapper().originalAttributes()),
        digitalSpecimenEvent.digitalMediaEvents());
  }

  public DigitalMediaEvent setExistingEventDatesMedia(
      DigitalMediaRecord currentDigitalMediaRecord,
      DigitalMediaEvent digitalMediaEvent) {
    var digitalMedia = digitalMediaEvent.digitalMediaWrapper().attributes();
    setEntityRelationshipDates(
        currentDigitalMediaRecord.attributes().getOdsHasEntityRelationships(),
        digitalMedia.getOdsHasEntityRelationships());
    // Set dcterms:created to original date
    digitalMedia.withDctermsCreated(
        currentDigitalMediaRecord.attributes().getDctermsCreated());
    // We create a new object because the events/wrappers are immutable, and we don't want the hash code to be out of sync
    return new DigitalMediaEvent(digitalMediaEvent.enrichmentList(),
        new DigitalMediaWrapper(
            digitalMediaEvent.digitalMediaWrapper().type(),
            digitalMedia,
            digitalMediaEvent.digitalMediaWrapper().originalAttributes()));
  }

  private void setEntityRelationshipDates(List<EntityRelationship> currentEntityRelationships,
      List<EntityRelationship> entityRelationships) {
    // Create a map with relatedResourceID as a key so we only compare potentially equal ERs
    // This reduces complexity compared to nested for-loops
    var currentEntityRelationshipsMap = currentEntityRelationships.stream()
        .collect(
            Collectors.toMap(EntityRelationship::getDwcRelatedResourceID, Function.identity(),
                (e1, e2) -> {
                  log.debug("Duplicate entity relationship found for resource");
                  if (!e1.getDwcRelationshipOfResource().contains("COL")) {
                    log.warn("Non-col entity relationship found for resource");
                  }
                  return e1;
                }));
    entityRelationships.forEach(entityRelationship -> {
      var currentEntityRelationship = currentEntityRelationshipsMap.get(
          entityRelationship.getDwcRelatedResourceID());
      if (entityRelationshipsAreEqual(currentEntityRelationship, entityRelationship)) {
        entityRelationship.setDwcRelationshipEstablishedDate(
            currentEntityRelationship.getDwcRelationshipEstablishedDate());
      }
    });
  }

  private List<EntityRelationship> addExistingMediaRelationships(
      MediaRelationshipProcessResult mediaRelationship,
      List<EntityRelationship> entityRelationships) {
    return Stream.concat(
        entityRelationships.stream(), mediaRelationship.unchangedRelationships().stream()
    ).toList();
  }

  private boolean specimensAreEqual(DigitalSpecimenRecord currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    if (currentDigitalSpecimen == null
        || currentDigitalSpecimen.digitalSpecimenWrapper().attributes() == null) {
      return false;
    }
    try {
      var jsonCurrentSpecimen = normaliseJsonNode(
          mapper.valueToTree(currentDigitalSpecimen.digitalSpecimenWrapper().attributes()), true);
      var jsonSpecimen = normaliseJsonNode(mapper.valueToTree(digitalSpecimenWrapper.attributes()),
          true);
      return isEqual(jsonCurrentSpecimen, jsonSpecimen, currentDigitalSpecimen.id());
    } catch (IOException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
  }

  private boolean mediaAreEqual(DigitalMediaRecord currentDigitalMedia,
      DigitalMediaWrapper digitalMedia) {
    if (currentDigitalMedia == null
        || currentDigitalMedia.attributes() == null) {
      return false;
    }
    try {
      var jsonCurrentMedia = normaliseJsonNode(
          mapper.valueToTree(currentDigitalMedia.attributes()), false);
      var jsonMedia = normaliseJsonNode(
          mapper.valueToTree(digitalMedia.attributes()), false);
      return isEqual(jsonCurrentMedia, jsonMedia, currentDigitalMedia.id());
    } catch (IOException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
  }

  private static boolean isEqual(JsonNode currentJson, JsonNode json, String id) {
    var isEqual = currentJson.equals(json);
    if (!isEqual) {
      log.debug("Object {} has changed. JsonDiff: {}", id,
          JsonDiff.asJson(currentJson, json));
    }
    return isEqual;
  }

  private boolean entityRelationshipsAreEqual(EntityRelationship currentEntityRelationship,
      EntityRelationship entityRelationship) {
    if (currentEntityRelationship == null || entityRelationship == null) {
      log.warn("Null ER!");
    }
    try {
      var jsonCurrentEntityRelationship = normaliseJsonNode(
          mapper.valueToTree(currentEntityRelationship), false);
      var jsonEntityRelationship = normaliseJsonNode(mapper.valueToTree(entityRelationship), false);
      return jsonCurrentEntityRelationship.equals(jsonEntityRelationship);
    } catch (IOException e) {
      log.error("Unable to serialize entity relationships", e);
      return false;
    }
  }

  private JsonNode normaliseJsonNode(JsonNode node, boolean isSpecimen)
      throws IOException {
    node = removeGeneratedTimestamps(node);
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(node));
    removeEntityRelationships(context, isSpecimen);
    return mapper.valueToTree(context.jsonString());
  }

  // Uses Json parser to remove generated timestamps
  // Only needs to scan JsonNode once
  public JsonNode removeGeneratedTimestamps(JsonNode node)
      throws IOException {
    StringWriter output = new StringWriter();
    var str = mapper.writeValueAsString(node);
    try (JsonParser parser = factory.createParser(new StringReader(str));
        JsonGenerator generator = factory.createGenerator(output)) {
      var skipNextField = false;
      while (!parser.isClosed()) {
        JsonToken token = parser.nextToken();
        if (token == null) {
          break;
        }
        switch (token) {
          case FIELD_NAME -> {
            String fieldName = parser.currentName();
            if (IGNORED_FIELDS.contains(fieldName)) {
              skipNextField = true;
            } else {
              generator.writeFieldName(fieldName);
              skipNextField = false;
            }
          }
          case START_OBJECT -> generator.writeStartObject();
          case END_OBJECT -> generator.writeEndObject();
          case START_ARRAY -> generator.writeStartArray();
          case END_ARRAY -> generator.writeEndArray();
          default -> {
            if (!skipNextField) {
              generator.copyCurrentEvent(parser);
            }
          }
        }
      }
    }
    return mapper.readTree(output.toString());
  }

  private static void removeEntityRelationships(DocumentContext context, boolean isSpecimen) {
    var filteredRelationship =
        isSpecimen ? HAS_MEDIA.getRelationshipName() : HAS_SPECIMEN.getRelationshipName();
    var filter = filter(where("dwc:relationshipOfResource").eq(filteredRelationship));
    context.delete("$['ods:hasEntityRelationships'][?]", filter);
  }

}
