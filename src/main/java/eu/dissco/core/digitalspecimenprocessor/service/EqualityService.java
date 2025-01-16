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
import eu.dissco.core.digitalspecimenprocessor.exception.EqualityParsingException;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.HashSet;
import java.util.List;
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
      DigitalSpecimenEvent digitalSpecimenEvent) throws EqualityParsingException {
    var digitalSpecimen = digitalSpecimenEvent.digitalSpecimenWrapper().attributes();
    var currentEntityRelationships = currentDigitalSpecimenWrapper.attributes()
        .getOdsHasEntityRelationships();
    var entityRelationships = digitalSpecimen.getOdsHasEntityRelationships();
    // Set Entity Relationship dates
    for (var entityRelationship : entityRelationships) {
      for (var currentEntityRelationship : currentEntityRelationships) {
        if (entityRelationshipsAreEqual(currentEntityRelationship, entityRelationship)) {
          entityRelationship.setDwcRelationshipEstablishedDate(
              currentEntityRelationship.getDwcRelationshipEstablishedDate());
        }
      }
    }
    // Set dcterms:created to original date
    digitalSpecimen.withDctermsCreated(
        currentDigitalSpecimenWrapper.attributes().getDctermsCreated());
    // We create a new object because the wrappers are immutable, and we don't want the hash code to be out of sync
    return new DigitalSpecimenEvent(digitalSpecimenEvent.enrichmentList(),
        new DigitalSpecimenWrapper(
            digitalSpecimenEvent.digitalSpecimenWrapper().physicalSpecimenID(),
            digitalSpecimenEvent.digitalSpecimenWrapper().type(),
            digitalSpecimen,
            digitalSpecimenEvent.digitalSpecimenWrapper().originalAttributes()),
        digitalSpecimenEvent.digitalMediaEvents());
  }

  private boolean specimensAreEqual(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    if (currentDigitalSpecimenWrapper == null
        || currentDigitalSpecimenWrapper.attributes() == null) {
      return false;
    }
    try {
      verifyOriginalData(currentDigitalSpecimenWrapper, digitalSpecimenWrapper);
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
      EntityRelationship entityRelationship) throws EqualityParsingException {
    try {
      var jsonCurrentEntityRelationship = normaliseJsonNode(
          mapper.valueToTree(currentEntityRelationship), false);
      var jsonEntityRelationship = normaliseJsonNode(mapper.valueToTree(entityRelationship), false);
      return jsonCurrentEntityRelationship.equals(jsonEntityRelationship);
    } catch (JsonProcessingException e) {
      log.error("Unable to serialize entity relationships", e);
      throw new EqualityParsingException();
    }
  }

  private JsonNode normaliseJsonNode(JsonNode node, boolean isSpecimen)
      throws JsonProcessingException {
    var context = using(jsonPathConfig).parse(mapper.writeValueAsString(node));
    removeGeneratedTimestamps(context);
    if (isSpecimen) {
      removeMediaEntityRelationships(context);
    }
    var jsonNode = mapper.readTree(context.jsonString());
    stripNulls(jsonNode);
    return jsonNode;
  }

  private static void removeGeneratedTimestamps(DocumentContext context) {
    for (var field : IGNORED_FIELDS) {
      var filter = filter(where(field).exists(true));
      // Find the paths for the fields we want to set to null
      var paths = new HashSet<String>(context.read("$..[?]", filter));
      for (var path : paths) {
        // We add the field name here because our jsonpath library omits it from its results
        var fullPath = path + "['" + field + "']";
        context.set(fullPath, null);
      }
    }
  }

  private static void removeMediaEntityRelationships(DocumentContext context) {
    var filter = filter(where("dwc:relationshipOfResource").eq(HAS_MEDIA.getName()));
    var paths = new HashSet<String>(
        context.read("$['ods:hasEntityRelationships'][?]", filter));
    for (var path : paths) {
      context.set(path, null);
    }
  }

  private static void stripNulls(JsonNode jsonNode) {
    var iterator = jsonNode.iterator();
    while (iterator.hasNext()) {
      var child = iterator.next();
      if (child.isNull()) {
        iterator.remove();
      } else {
        stripNulls(child);
      }
    }
  }

  private static void verifyOriginalData(DigitalSpecimenWrapper currentDigitalSpecimenWrapper,
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var currentOriginalData = currentDigitalSpecimenWrapper.originalAttributes();
    var originalData = digitalSpecimenWrapper.originalAttributes();
    if (currentOriginalData != null && !currentOriginalData.equals(originalData)) {
      log.debug(
          "Original data for specimen with physical id {} has changed. Ignoring new original data.",
          digitalSpecimenWrapper.physicalSpecimenID());
    }
  }

}
