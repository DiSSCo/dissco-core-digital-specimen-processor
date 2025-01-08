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
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
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
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    if (currentDigitalSpecimenWrapper == null) {
      return false;
    }
    try {
      var jsonCurrentSpecimen = normalizeJsonNode(mapper.createObjectNode()
          .set("root", mapper.valueToTree(currentDigitalSpecimenWrapper.attributes())));
      var jsonSpecimen = normalizeJsonNode(mapper.createObjectNode()
          .set("root", mapper.valueToTree(digitalSpecimenWrapper.attributes())));
      verifyOriginalData(currentDigitalSpecimenWrapper, digitalSpecimenWrapper);
      var isEqual = jsonCurrentSpecimen.equals(jsonSpecimen);
      if (!isEqual) {
        log.info("Specimen {} has changed. JsonDiff: {}",
            currentDigitalSpecimenWrapper.physicalSpecimenID(),
            JsonDiff.asJson(jsonCurrentSpecimen, jsonSpecimen));
      }
      return isEqual;
    } catch (JsonProcessingException e) {
      log.error("Unable to re-serialize JSON. Can not determine equality.", e);
      return false;
    }
  }

  private JsonNode normalizeJsonNode(JsonNode specimen) throws JsonProcessingException {
    var specimenString = mapper.writeValueAsString(specimen);
    var context = using(jsonPathConfig).parse(specimenString);
    removeGeneratedTimestamps(context);
    removeMediaEntityRelationships(context);
    var jsonNode = mapper.readTree(context.jsonString());
    stripNulls(jsonNode);
    return jsonNode;
  }

  private static void removeGeneratedTimestamps(DocumentContext context) {
    for (var field : IGNORED_FIELDS) {
      var filter = filter(where(field).exists(true));
      var paths = new HashSet<String>(context.read("$..*[?]", filter));
      for (var path : paths) {
        var fullPath = path + "['" + field + "']";
        context.set(fullPath, null);
      }
    }
  }

  private static void removeMediaEntityRelationships(DocumentContext context) {
    var filter = filter(where("dwc:relationshipOfResource").eq(HAS_MEDIA.getName()));
    var paths = new HashSet<String>(
        context.read("$['root']['ods:hasEntityRelationships'][?]", filter));
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
      log.info(
          "Original data for specimen with physical id {} has changed. Ignoring new original data.",
          digitalSpecimenWrapper.physicalSpecimenID());
    }
  }

}
