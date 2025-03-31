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
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.HashSet;
import java.util.List;
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

  public boolean isEqual(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper,
      DigitalMediaProcessResult digitalMediaProcessResult) {
    return specimensAreEqual(currentDigitalSpecimenWrapper, digitalSpecimenWrapper)
        && digitalMediaProcessResult.newMedia().isEmpty()
        && digitalMediaProcessResult.tombstoneMedia().isEmpty();
  }

  public DigitalSpecimenEvent setEventDates(
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
      var isEqual = jsonCurrentSpecimen.equals(jsonSpecimen);
      if (!isEqual) {
        log.debug("Specimen {} has changed. JsonDiff: {}",
            currentDigitalSpecimenWrapper.attributes().getDctermsIdentifier(),
            JsonDiff.asJson(jsonCurrentSpecimen, jsonSpecimen));
      }
      return isEqual;
    } catch (JsonProcessingException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
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
