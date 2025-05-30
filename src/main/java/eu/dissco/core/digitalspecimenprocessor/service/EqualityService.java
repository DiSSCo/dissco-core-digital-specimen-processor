package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_SPECIMEN;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.fge.jsonpatch.diff.JsonDiff;
import eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
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

  private final ObjectMapper mapper;
  private static final Set<String> IGNORED_FIELDS = Set.of(
      "dcterms:created",
      "dcterms:modified",
      "dwc:relationshipEstablishedDate"
  );

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
    digitalSpecimen.setOdsHasEntityRelationships(
        addExistingMediaRelationships(mediaRelationships, // add existing media ers
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
    var jsonCurrentSpecimen = normaliseJsonNodeSpecimen(
        currentDigitalSpecimen.digitalSpecimenWrapper().attributes());
    var jsonSpecimen = normaliseJsonNodeSpecimen(digitalSpecimenWrapper.attributes());
    return isEqual(jsonCurrentSpecimen, jsonSpecimen, currentDigitalSpecimen.id());
  }

  private boolean mediaAreEqual(DigitalMediaRecord currentDigitalMedia,
      DigitalMediaWrapper digitalMedia) {
    if (currentDigitalMedia == null
        || currentDigitalMedia.attributes() == null) {
      return false;
    }
    var jsonCurrentMedia = normaliseJsonNodeMedia(currentDigitalMedia.attributes());
    var jsonMedia = normaliseJsonNodeMedia(digitalMedia.attributes());
    return isEqual(jsonCurrentMedia, jsonMedia, currentDigitalMedia.id());

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
      return currentEntityRelationship == null && entityRelationship == null;
    }
    var jsonCurrentEntityRelationship = removeGeneratedTimestamps(
        mapper.valueToTree(currentEntityRelationship));
    var jsonEntityRelationship = removeGeneratedTimestamps(mapper.valueToTree(entityRelationship));
    return jsonCurrentEntityRelationship.equals(jsonEntityRelationship);

  }

  private JsonNode normaliseJsonNodeSpecimen(DigitalSpecimen digitalSpecimen) {
    var node = mapper.valueToTree(digitalSpecimen);
    removeEntityRelationships((ObjectNode) node, HAS_MEDIA);
    return removeGeneratedTimestamps(node);
  }

  private JsonNode normaliseJsonNodeMedia(DigitalMedia digitalMedia) {
    var node = mapper.valueToTree(digitalMedia);
    removeEntityRelationships((ObjectNode) node, HAS_SPECIMEN);
    return removeGeneratedTimestamps(node);
  }

  public JsonNode removeGeneratedTimestamps(JsonNode node) {
    if (node.isObject()) {
      ObjectNode result = mapper.createObjectNode();
      node.fields().forEachRemaining(entry -> {
        if (!IGNORED_FIELDS.contains(entry.getKey())) {
          result.set(entry.getKey(), removeGeneratedTimestamps(entry.getValue()));
        }
      });
      return result;
    } else if (node.isArray()) {
      ArrayNode result = mapper.createArrayNode();
      node.forEach(element -> result.add(removeGeneratedTimestamps(element)));
      return result;
    } else {
      return node;
    }
  }

  private void removeEntityRelationships(ObjectNode node,
      EntityRelationshipType targetRelationship) {
    var entityRelationshipArray = (ArrayNode) node.get("ods:hasEntityRelationships");
    if (entityRelationshipArray == null) {
      return;
    }
    var filteredEntityRelationships = mapper.createArrayNode();
    for (var er : entityRelationshipArray) {
      if (!targetRelationship.getRelationshipName()
          .equals(er.get("dwc:relationshipOfResource").asText())) {
        filteredEntityRelationships.add(er);
      }
    }
    node.set("ods:hasEntityRelationships", filteredEntityRelationships);
  }

}
