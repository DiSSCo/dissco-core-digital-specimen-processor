package eu.dissco.core.digitalspecimenprocessor.service;

import static com.jayway.jsonpath.Criteria.where;
import static com.jayway.jsonpath.Filter.filter;
import static com.jayway.jsonpath.JsonPath.using;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;

import com.fasterxml.jackson.core.JsonProcessingException;
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
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMqProperties.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class EqualityService {

  private final Configuration jsonPathConfig;
  private final ObjectMapper mapper;
  private static final List<String> IGNORED_FIELDS = List.of(
      "dcterms:created",
      "dcterms:modified",
      "dwc:relationshipEstablishedDate"
  );

  public boolean specimensAreEqual(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper,
      MediaRelationshipProcessResult mediaRelationshipProcessResult) {
    return specimensAreEqual(currentDigitalSpecimenWrapper, digitalSpecimenWrapper)
        && mediaRelationshipProcessResult.newLinkedObjects().isEmpty()
        && mediaRelationshipProcessResult.tombstonedRelationships().isEmpty();
  }

  public boolean mediaAreEqual(DigitalMediaRecord currentDigitalMedia,
      DigitalMediaWrapper digitalMedia,
      Set<String> newSpecimenRelationships) {
   return mediaAreEqual(currentDigitalMedia, digitalMedia)
       && newSpecimenRelationships.isEmpty();
  }

  public DigitalSpecimenEvent setEventDatesSpecimen(
      DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenEvent digitalSpecimenEvent) {
    var digitalSpecimen = digitalSpecimenEvent.digitalSpecimenWrapper().attributes();
    setEntityRelationshipDates(
        currentDigitalSpecimenWrapper.attributes().getOdsHasEntityRelationships(),
        digitalSpecimen.getOdsHasEntityRelationships());
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

  public DigitalMediaEvent setEventDatesMedia(
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
            digitalMediaEvent.digitalMediaWrapper().accessUri(),
            digitalMedia,
            digitalMediaEvent.digitalMediaWrapper().originalAttributes()));
  }

  private void setEntityRelationshipDates(List<EntityRelationship> currentEntityRelationships,
      List<EntityRelationship> entityRelationships) {
    // Create a map with relatedResourceID as a key so we only compare potentially equal ERs
    // This reduces complexity compared to nested for-loops
    var currentEntityRelationshipsMap = currentEntityRelationships.stream()
        .collect(
            Collectors.toMap(EntityRelationship::getDwcRelatedResourceID, Function.identity()));
    entityRelationships.forEach(entityRelationship -> {
      var currentEntityRelationship = currentEntityRelationshipsMap.get(
          entityRelationship.getDwcRelatedResourceID());
      if (entityRelationshipsAreEqual(currentEntityRelationship, entityRelationship)) {
        entityRelationship.setDwcRelationshipEstablishedDate(
            currentEntityRelationship.getDwcRelationshipEstablishedDate());
      }
    });
  }

  private boolean specimensAreEqual(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    if (currentDigitalSpecimenWrapper == null
        || currentDigitalSpecimenWrapper.attributes() == null) {
      return false;
    }
    try {
      var jsonCurrentSpecimen = normaliseJsonNode(
          mapper.valueToTree(currentDigitalSpecimenWrapper.attributes()), true);
      var jsonSpecimen = normaliseJsonNode(mapper.valueToTree(digitalSpecimenWrapper.attributes()),
          true);
      return isEqual(jsonCurrentSpecimen, jsonSpecimen, digitalSpecimenWrapper.attributes().getId());
    } catch (JsonProcessingException e) {
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
    } catch (JsonProcessingException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
  }

  private static boolean isEqual(JsonNode currentJson, JsonNode json, String id){
    var isEqual = currentJson.equals(json);
    if (!isEqual) {
      log.debug("Media {} has changed. JsonDiff: {}", id,
          JsonDiff.asJson(currentJson, json));
    }
    return isEqual;
  }

  private boolean entityRelationshipsAreEqual(EntityRelationship currentEntityRelationship,
      EntityRelationship entityRelationship) {
    try {
      var jsonCurrentEntityRelationship = normaliseJsonNode(
          mapper.valueToTree(currentEntityRelationship), false);
      var jsonEntityRelationship = normaliseJsonNode(mapper.valueToTree(entityRelationship), false);
      return jsonCurrentEntityRelationship.equals(jsonEntityRelationship);
    } catch (JsonProcessingException e) {
      log.error("Unable to serialize entity relationships", e);
      return false;
    }
  }

  private JsonNode normaliseJsonNode(JsonNode node, boolean isSpecimen)
      throws JsonProcessingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(node));
    removeGeneratedTimestamps(context);
    if (isSpecimen) {
      removeMediaEntityRelationships(context);
    }
    return mapper.valueToTree(context.jsonString());
  }

  private static void removeGeneratedTimestamps(DocumentContext context) {
    IGNORED_FIELDS.forEach(field -> {
      // Find paths of target field
      var paths = new HashSet<String>(context.read("$..[?(@." + field + ")]"));
      // Set each value of the given path to null
      paths.forEach(path -> {
        var fullPath = path + "['" + field + "']";
        context.delete(fullPath);
      });
    });
  }

  private static void removeMediaEntityRelationships(DocumentContext context) {
    var filter = filter(where("dwc:relationshipOfResource").eq(HAS_MEDIA.getName()));
    new HashSet<String>(
        context.read("$['ods:hasEntityRelationships'][?]", filter))
        .forEach(context::delete);
  }

}
