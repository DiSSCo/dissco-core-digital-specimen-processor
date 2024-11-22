package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.PROCESSING_SERVICE;
import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LINKED_DO_PID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.LINKED_DO_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_HOST_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MEDIA_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.MIME_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_NAME;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.SCHEMA_SOFTWARE_APPLICATION;
import static eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType.DOI;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationBody;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationProcessingRequest.OaMotivation;
import eu.dissco.core.digitalspecimenprocessor.schema.AnnotationTarget;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsLivingOrPreserved;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType;
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class TestUtils {

  public static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  public static final String HANDLE = "20.5000.1025/V1Z-176-LL4";
  public static final String DOI_PREFIX = "https://doi.org/";
  public static final String SECOND_HANDLE = "20.5000.1025/XXX-XXX-XXX";
  public static final String THIRD_HANDLE = "20.5000.1025/YYY-YYY-YYY";
  public static final String APP_HANDLE = "https://hdl.handle.net/TEST/123-123-123";
  public static final String APP_NAME = "dissco-digital-specimen-processor";
  public static final int MIDS_LEVEL = 1;
  public static final int VERSION = 1;
  public static final Instant CREATED = Instant.parse("2022-11-01T09:59:24.000Z");
  public static final String MAS = "OCR";
  public static final String TYPE = "https://doi.org/21.T11148/894b1e6cad57e921764e";
  public static final String TYPE_MEDIA = "https://doi.org/21.T11148/bbad8c4e101e8af01115";
  public static final String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
  public static final String PHYSICAL_SPECIMEN_ID_ALT = "A second specimen";
  public static final OdsPhysicalSpecimenIDType PHYSICAL_SPECIMEN_TYPE = OdsPhysicalSpecimenIDType.GLOBAL;
  public static final String SPECIMEN_NAME = "Biota";
  public static final String ANOTHER_SPECIMEN_NAME = "Another SpecimenName";
  public static final String ORGANISATION_ID = "https://ror.org/0443cwa12";
  public static final String ANOTHER_ORGANISATION = "Another organisation";
  public static final String DATASET_ID = null;
  public static final String PHYSICAL_SPECIMEN_COLLECTION = null;
  public static final String SOURCE_SYSTEM_ID = "https://hdl.handle.net/TEST/57Z-6PC-64W";
  public static final String SOURCE_SYSTEM_NAME = "A very nice source system";
  public static final JsonNode ORIGINAL_DATA = generateSpecimenOriginalData();
  public static final OdsTopicDiscipline TOPIC_DISCIPLINE = OdsTopicDiscipline.BOTANY;
  public static final String MEDIA_URL = "https://an-image.org";
  public static final String MEDIA_URL_ALT = MEDIA_URL + "/2";
  public static final String MEDIA_PID = "20.5000.1025/ZZZ-ZZZ-ZZZ";
  public static final String MEDIA_PID_ALT = "20.5000.1025/AAA-AAA-AAA";


  public static final String SPECIMEN_BASE_URL = "https://doi.org/";

  public static JsonNode generateSpecimenOriginalData() {
    try {
      return MAPPER.readTree(
          """
              {"abcd:unitID": "152-4972",
                "abcd:sourceID": "GIT",
                "abcd:unitGUID": "https://geocollections.info/specimen/23646",
                "abcd:recordURI": "https://geocollections.info/specimen/23646",
                "abcd:recordBasis": "FossilSpecimen",
                "abcd:unitIDNumeric": 23646,
                "abcd:dateLastEdited": "2004-06-09T10:17:54.000+00:00",
                "abcd:kindOfUnit/0/value": "",
                "abcd:sourceInstitutionID": "Department of Geology, TalTech",
                "abcd:kindOfUnit/0/language": "en",
                "abcd:gathering/country/name/value": "Estonia",
                "abcd:gathering/localityText/value": "Laeva 297 borehole",
                "abcd:gathering/country/iso3166Code": "EE",
                "abcd:gathering/localityText/language": "en",
                "abcd:gathering/altitude/measurementOrFactText/value": "39.9",
                "abcd:identifications/identification/0/preferredFlag": true,
                "abcd:gathering/depth/measurementOrFactAtomised/lowerValue/value": "165",
                "abcd:gathering/depth/measurementOrFactAtomised/unitOfMeasurement": "m",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/spatialDatum": "WGS84",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/term": "Pirgu Stage",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/term": "Katian",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/latitudeDecimal": 58.489269,
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/longitudeDecimal": 26.385719,
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/language": "en",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/language": "en",
                "abcd:identifications/identification/0/result/taxonIdentified/scientificName/fullScientificNameString": "Biota",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronostratigraphicName": "Pirgu Stage",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronoStratigraphicDivision": "Stage"
              }"""
      );
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(int version, boolean hasMedia) {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        version,
        CREATED,
        givenDigitalSpecimenWrapper(hasMedia)
    );
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr() {
    return givenDigitalSpecimenRecordWithMediaEr(HANDLE, PHYSICAL_SPECIMEN_ID, false);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship, int version) {
    return givenDigitalSpecimenRecordWithMediaEr(handle, physicalId, addOtherEntityRelationship,
        version, MEDIA_PID);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship, int version, String mediaId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        version,
        CREATED,
        givenDigitalSpecimenWrapperWithMediaEr(physicalId, addOtherEntityRelationship, mediaId));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordWithMediaEr(String handle,
      String physicalId, boolean addOtherEntityRelationship) {
    return givenDigitalSpecimenRecordWithMediaEr(handle, physicalId, addOtherEntityRelationship, 1);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord() {
    return givenDigitalSpecimenRecord(VERSION, false);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord() {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID, false);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenWithEntityRelationship() {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID, true,
            true)
    );
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String organisation) {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, organisation, false);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String handle,
      String specimenName, String organisation, boolean hasMedia) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, specimenName, organisation, false,
            hasMedia)
    );
  }

  public static DigitalSpecimenRecord givenDifferentUnequalSpecimen(String handle,
      String physicalIdentifier) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalIdentifier, ANOTHER_SPECIMEN_NAME,
            ANOTHER_ORGANISATION, false, false));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle,
      String physicalSpecimenId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false,
            false)
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false,
            true),
        List.of(givenDigitalMediaEvent(physicalSpecimenId))
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEventWithMediaEr(
      String physicalSpecimenId) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapperWithMediaEr(physicalSpecimenId, false),
        List.of(givenDigitalMediaEvent(physicalSpecimenId))
    );
  }

  public static DigitalMediaProcessResult givenEmptyMediaProcessResult() {
    return new DigitalMediaProcessResult(
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
  }

  public static Map<String, DigitalMediaProcessResult> givenEmptyMediaProcessResultMap(
      List<String> handles) {
    var processMap = new HashMap<String, DigitalMediaProcessResult>();
    for (var handle : handles) {
      processMap.put(handle, givenEmptyMediaProcessResult());
    }
    return processMap;
  }

  public static Map<String, DigitalMediaProcessResult> givenEmptyMediaProcessResultMap() {
    return Map.of(HANDLE, givenEmptyMediaProcessResult());
  }


  public static DigitalMediaProcessResult givenMediaProcessResultNew(
      DigitalSpecimenEvent event) {
    return new DigitalMediaProcessResult(
        Collections.emptyList(),
        Collections.emptyList(),
        event.digitalMediaEvents()
    );
  }

  public static Map<String, DigitalMediaProcessResult> givenMediaProcessResultMapNew(
      Map<String, DigitalSpecimenEvent> events) {
    return events.entrySet().stream()
        .collect(Collectors.toMap(
            Entry::getKey,
            e -> givenMediaProcessResultNew(e.getValue())
        ));
  }

  public static DigitalMediaProcessResult givenMediaProcessResultUnchanged(
      List<DigitalSpecimenEvent> events) {
    return new DigitalMediaProcessResult(
        events.stream()
            .map(
                event -> event.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships())
            .flatMap(List::stream)
            .filter(entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
                .equals("hasDigitalMedia"))
            .toList(),
        Collections.emptyList(),
        Collections.emptyList()
    );
  }


  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return givenDigitalSpecimenEvent(false);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia,
      boolean entityRelationship) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(entityRelationship, hasMedia),
        hasMedia ? List.of(givenDigitalMediaEvent()) : List.of()
    );
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperWithMediaEr(String physicalId,
      Boolean addOtherEntityRelationship) {
    return givenDigitalSpecimenWrapperWithMediaEr(physicalId, addOtherEntityRelationship,
        MEDIA_PID);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperWithMediaEr(String physicalId,
      Boolean addOtherEntityRelationship, String mediaId) {
    var attributes = givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true,
        addOtherEntityRelationship, true);
    var entityRelationships = new ArrayList<>(attributes.getOdsHasEntityRelationships());
    entityRelationships.add(
        new EntityRelationship()
            .withType("ods:EntityRelationship")
            .withDwcRelationshipEstablishedDate(Date.from(CREATED))
            .withDwcRelationshipOfResource(HAS_MEDIA.getName())
            .withOdsHasAgents(List.of(createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE,
                DOI, SCHEMA_SOFTWARE_APPLICATION)))
            .withDwcRelatedResourceID(DOI_PREFIX + mediaId)
            .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + mediaId))
    );
    attributes.setOdsHasEntityRelationships(entityRelationships);
    return new DigitalSpecimenWrapper(physicalId, TYPE, attributes,
        ORIGINAL_DATA);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia) {
    return givenDigitalSpecimenEvent(hasMedia, false);
  }

  public static DigitalMediaEventWithoutDOI givenDigitalMediaEvent(String specimenId,
      String mediaUrl) {
    return new DigitalMediaEventWithoutDOI(
        List.of("image-metadata"),
        new DigitalMediaWithoutDOI(
            "StillImage",
            specimenId,
            new DigitalMedia().withAcAccessURI(mediaUrl).withOdsOrganisationID(ORGANISATION_ID),
            MAPPER.createObjectNode()
        )
    );
  }

  public static DigitalMediaEventWithoutDOI givenDigitalMediaEvent(String specimenId) {
    return givenDigitalMediaEvent(specimenId, MEDIA_URL);
  }

  public static DigitalMediaEventWithoutDOI givenDigitalMediaEvent() {
    return givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship() {
    return givenDigitalMediaEventWithRelationship(null);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship(String id) {
    var digitalMedia = new DigitalMedia().withAcAccessURI(MEDIA_URL)
        .withId(id)
        .withDctermsIdentifier(id)
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsHasEntityRelationships(
            List.of(new EntityRelationship()
                .withType("ods:EntityRelationship")
                .withDwcRelationshipOfResource("hasDigitalSpecimen")
                .withDwcRelationshipEstablishedDate(Date.from(CREATED))
                .withDwcRelatedResourceID(DOI_PREFIX + HANDLE)
                .withOdsRelatedResourceURI(URI.create(DOI_PREFIX + HANDLE))
                .withOdsHasAgents(List.of(createMachineAgent(APP_NAME, APP_HANDLE,
                    PROCESSING_SERVICE, DOI, SCHEMA_SOFTWARE_APPLICATION)))));
    return new DigitalMediaEvent(
        List.of("image-metadata"),
        new DigitalMediaWrapper(
            "StillImage",
            HANDLE,
            digitalMedia,
            MAPPER.createObjectNode()
        ));
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper() {
    return givenDigitalSpecimenWrapper(false, false);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(boolean hasMedia) {
    return givenDigitalSpecimenWrapper(false, hasMedia);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(boolean entityRelationship,
      boolean hasMedia) {
    return givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID,
        entityRelationship, hasMedia);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(String physicalSpecimenId,
      String specimenName, String organisation, boolean entityRelationship, boolean hasMedia) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        TYPE,
        givenAttributes(specimenName, organisation, true, entityRelationship, hasMedia),
        ORIGINAL_DATA
    );
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecordNoOriginalData() {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapperNoOriginalData()
    );
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapperNoOriginalData() {
    return new DigitalSpecimenWrapper(
        PHYSICAL_SPECIMEN_ID,
        TYPE,
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false, false),
        MAPPER.createObjectNode()
    );
  }

  public static Map<String, String> givenHandleComponentResponse() {
    return givenHandleComponentResponse(List.of(PHYSICAL_SPECIMEN_ID), List.of(HANDLE));
  }

  public static Map<String, String> givenHandleComponentResponse(List<String> physIds,
      List<String> handles) {
    assert (physIds.size() == handles.size());
    Map<String, String> pidMap = new HashMap<>();
    for (int i = 0; i < physIds.size(); i++) {
      pidMap.put(physIds.get(i), handles.get(i));
    }
    return pidMap;
  }

  public static Map<String, String> givenHandleComponentResponse(
      List<DigitalSpecimenRecord> records) {
    Map<String, String> response = new HashMap<>();
    for (var specimenRecord : records) {
      response.put(specimenRecord.digitalSpecimenWrapper().physicalSpecimenID(),
          specimenRecord.id());
    }
    return response;
  }

  public static Map<DigitalMediaKey, String> givenHandleComponentResponseMedia(
      List<DigitalSpecimenRecord> records) {
    Map<DigitalMediaKey, String> response = new HashMap<>();
    for (var specimenRecord : records) {
      response.put(new DigitalMediaKey(null, null),
          specimenRecord.id());
    }
    return response;
  }

  public static JsonNode givenUpdateHandleRequest() throws Exception {
    return givenUpdateHandleRequest(null);
  }

  public static JsonNode givenUpdateHandleRequest(Boolean markedAsType) throws Exception {
    var attributes = givenHandleAttributes(markedAsType);
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("id", HANDLE)
            .put("type", TYPE)
            .set("attributes", attributes));
  }

  public static JsonNode givenHandleMediaRequest() {
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE_MEDIA)
            .set("attributes", givenHandleMediaRequestAttributes()));
  }

  public static JsonNode givenHandleMediaRequestAttributes() {
    return MAPPER.createObjectNode()
        .put(MEDIA_HOST.getAttribute(), ORGANISATION_ID)
        .put(MEDIA_HOST_NAME.getAttribute(), (String) null)
        .put(LINKED_DO_PID.getAttribute(), HANDLE)
        .put(LINKED_DO_TYPE.getAttribute(), TYPE)
        .put(PRIMARY_MEDIA_ID.getAttribute(), MEDIA_URL)
        .put(PRIMARY_MEDIA_ID_TYPE.getAttribute(), "Resolvable")
        .put(PRIMARY_MEDIA_ID_NAME.getAttribute(), "ac:accessURI")
        .put(MEDIA_TYPE.getAttribute(), "image")
        .put(MIME_TYPE.getAttribute(), (String) null);
  }


  public static JsonNode givenHandleRequest() throws Exception {
    return givenHandleRequest(null);
  }

  public static JsonNode givenHandleRequest(Boolean markedAsType) throws Exception {
    var attributes = (ObjectNode) givenHandleAttributes(markedAsType);
    return MAPPER.createObjectNode()
        .set("data", MAPPER.createObjectNode()
            .put("type", TYPE)
            .set("attributes", attributes));
  }

  public static JsonNode givenHandleAttributes(Boolean markedAsType) throws Exception {
    var attributes = (ObjectNode) MAPPER.readTree("""
        {
          "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
          "specimenHost": "https://ror.org/0443cwa12",
          "specimenHostName": "National Museum of Natural History",
          "topicDiscipline": "Botany",
          "referentName": "Biota",
          "livingOrPreserved": "Preserved",
          "otherSpecimenIds":[{"identifierType":"physical specimen identifier","identifierValue": "https://geocollections.info/specimen/23602","resolvable":false}]
        }
        """);
    if (markedAsType != null) {
      attributes.put("markedAsType", markedAsType);
    }
    return attributes;
  }

  public static JsonNode givenHandleRequestMin() throws Exception {
    return MAPPER.readTree("""
        {
          "data": {
            "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
            "attributes": {
              "normalisedPrimarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "specimenHost": "https://ror.org/0443cwa12",
              "otherSpecimenIds":[{"identifierType":"physical specimen identifier","identifierValue": "https://geocollections.info/specimen/23602","resolvable":false}]
            }
          }
        }
        """);
  }

  public static DigitalSpecimen givenAttributes(
      String specimenName, String organisation, Boolean markedAsType,
      boolean addEntityRelationShip, boolean containsMedia) {
    var ds = new DigitalSpecimen()
        .withOdsOrganisationID(organisation)
        .withOdsOrganisationName("National Museum of Natural History")
        .withOdsPhysicalSpecimenIDType(PHYSICAL_SPECIMEN_TYPE)
        .withOdsPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
        .withOdsNormalisedPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
        .withOdsSpecimenName(specimenName)
        .withOdsTopicDiscipline(TOPIC_DISCIPLINE)
        .withOdsSourceSystemID(SOURCE_SYSTEM_ID)
        .withOdsSourceSystemName(SOURCE_SYSTEM_NAME)
        .withOdsLivingOrPreserved(OdsLivingOrPreserved.PRESERVED)
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/")
        .withDwcCollectionID(PHYSICAL_SPECIMEN_COLLECTION)
        .withDwcDatasetName(DATASET_ID)
        .withOdsIsMarkedAsType(markedAsType)
        .withOdsIsKnownToContainMedia(containsMedia)
        .withDctermsModified("2022-11-01T09:59:24.000Z");
    if (addEntityRelationShip) {
      ds.withOdsHasEntityRelationships(
          List.of(new EntityRelationship()
              .withDwcRelationshipOfResource("hasDigitalSpecimen")
              .withDwcRelatedResourceID(SPECIMEN_BASE_URL + HANDLE)
              .withDwcRelationshipEstablishedDate(new Date())));
    }
    return ds;
  }

  public static DigitalSpecimen givenAttributesPlusIdentifier(String specimenName,
      String organisation, Boolean markedAsType) {
    return givenAttributes(specimenName, organisation, markedAsType, false, false)
        .withOdsHasIdentifiers(List.of(
            new Identifier().withDctermsTitle("Specimen label").withDctermsIdentifier(HANDLE)));
  }

  public static AutoAcceptedAnnotation givenAutoAcceptedAnnotation(
      AnnotationProcessingRequest annotation) {
    return new AutoAcceptedAnnotation(
        createMachineAgent(APP_NAME, APP_HANDLE, PROCESSING_SERVICE, DOI,
            SCHEMA_SOFTWARE_APPLICATION), annotation);
  }

  public static AnnotationProcessingRequest givenNewAcceptedAnnotation()
      throws JsonProcessingException {
    return new AnnotationProcessingRequest()
        .withOaMotivation(OaMotivation.ODS_ADDING)
        .withOaMotivatedBy("New information received from Source System with id: "
            + SOURCE_SYSTEM_ID)
        .withOaHasBody(new AnnotationBody()
            .withType("oa:TextualBody")
            .withOaValue(List.of(MAPPER.writeValueAsString(
                DigitalSpecimenUtils.flattenToDigitalSpecimen(givenDigitalSpecimenRecord()))))
            .withDctermsReferences(SOURCE_SYSTEM_ID))
        .withDctermsCreated(Date.from(CREATED))
        .withDctermsCreator(createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID, SOURCE_SYSTEM,
            DctermsType.HANDLE, SCHEMA_SOFTWARE_APPLICATION))
        .withOaHasTarget(new AnnotationTarget()
            .withId(DOI_PREFIX + HANDLE)
            .withDctermsIdentifier(DOI_PREFIX + HANDLE)
            .withType(TYPE)
            .withOdsFdoType("ods:DigitalSpecimen")
            .withOaHasSelector(new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class", "$")));

  }

  public static JsonNode givenJsonPatch() throws JsonProcessingException {
    return MAPPER.readTree(
        "[{\"op\":\"replace\",\"path\":\"/ods:specimenName\",\"value\":\"Biota\"}]");
  }

  public static Map<DigitalMediaKey, String> givenMediaPidResponse() {
    return Map.of(new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID);
  }

}
