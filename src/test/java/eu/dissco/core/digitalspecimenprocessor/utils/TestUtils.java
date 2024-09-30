package eu.dissco.core.digitalspecimenprocessor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
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
import eu.dissco.core.digitalspecimenprocessor.schema.OaHasSelector;
import eu.dissco.core.digitalspecimenprocessor.util.DigitalSpecimenUtils;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public static final String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
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
  public static final String MEDIA_PID = "20.5000.1025/ZZZ-ZZZ-ZZZ";

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

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(int version) {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        version,
        CREATED,
        givenDigitalSpecimenWrapper()
    );
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord() {
    return givenDigitalSpecimenRecord(VERSION);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord() {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, ORGANISATION_ID);
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenWithEntityRelationship() {
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID, true)
    );
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String organisation) {
    return givenUnequalDigitalSpecimenRecord(HANDLE, ANOTHER_SPECIMEN_NAME, organisation);
  }

  public static DigitalSpecimenRecord givenUnequalDigitalSpecimenRecord(String handle,
      String specimenName, String organisation) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, specimenName, organisation, false)
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
            ANOTHER_ORGANISATION, false));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle,
      String physicalSpecimenId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false)
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID, false),
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent())
    );
  }


  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return givenDigitalSpecimenEvent(false);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia,
      boolean entityRelationship) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(entityRelationship),
        hasMedia ? List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent()) : List.of()
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia) {
    return givenDigitalSpecimenEvent(hasMedia, false);
  }

  public static DigitalMediaEventWithoutDOI givenDigitalMediaEvent() {
    return new DigitalMediaEventWithoutDOI(
        List.of("image-metadata"),
        new DigitalMediaWithoutDOI(
            "StillImage",
            PHYSICAL_SPECIMEN_ID,
            new DigitalMedia().withAcAccessURI(MEDIA_URL).withOdsOrganisationID(ORGANISATION_ID),
            MAPPER.createObjectNode()
        )
    );
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship(){
    return givenDigitalMediaEventWithRelationship(null);
  }

  public static DigitalMediaEvent givenDigitalMediaEventWithRelationship(String id) {
    return new DigitalMediaEvent(
        List.of("image-metadata"),
        new DigitalMediaWrapper(
            "StillImage",
            HANDLE,
            new DigitalMedia().withAcAccessURI(MEDIA_URL)
                .withId(id)
                .withOdsID(id)
                .withOdsOrganisationID(ORGANISATION_ID)
                .withOdsHasEntityRelationship(
                List.of(new EntityRelationship()
                    .withType("ods:EntityRelationship")
                    .withDwcRelationshipOfResource("hasDigitalSpecimen")
                    .withDwcRelationshipEstablishedDate(Date.from(Instant.now()))
                    .withDwcRelationshipAccordingTo("dissco-digital-specimen-processor")
                    .withOdsRelationshipAccordingToAgent(new Agent()
                        .withId("https://hdl.handle.net/TEST/123-123-123")
                        .withType(Type.AS_APPLICATION)
                        .withSchemaName("dissco-digital-specimen-processor"))
                    .withDwcRelatedResourceID(SPECIMEN_BASE_URL + "20.5000.1025/V1Z-176-LL4"))
            ),
            MAPPER.createObjectNode()
        ));
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper() {
    return givenDigitalSpecimenWrapper(false);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(boolean entityRelationship) {
    return givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID,
        entityRelationship);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(String physicalSpecimenId,
      String specimenName, String organisation, boolean entityRelationship) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        TYPE,
        givenAttributes(specimenName, organisation, true, entityRelationship),
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
        givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false),
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

  public static JsonNode givenUpdateHandleRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data": 
            {
              "id": "20.5000.1025/V1Z-176-LL4",
              "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
              "attributes": {
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "normalisedPrimarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "primarySpecimenObjectIdType":"Global",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "primarySpecimenObjectIdType": "Global",
                "topicDiscipline": "Botany",
                "referentName": "Biota",
                "livingOrPreserved": "Preserved",
                "markedAsType": true
              }
            }
        }
        """);
  }

  public static JsonNode givenHandleRequestFullTypeStatus() throws Exception {
    return MAPPER.readTree("""
        {
          "data": {
            "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
            "attributes": {
              "issuedForAgent": "https://ror.org/0566bfb96",
              "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "normalisedPrimarySpecimenObjectId":"https://geocollections.info/specimen/23602",
              "primarySpecimenObjectIdType": "Global",
              "specimenHost": "https://ror.org/0443cwa12",
              "specimenHostName": "National Museum of Natural History",
              "topicDiscipline": "Botany",
              "referentName": "Biota",
              "livingOrPreserved": "Preserved",
              "markedAsType":true
            }
          }
        }""");
  }

  public static JsonNode givenHandleRequestMin() throws Exception {
    return MAPPER.readTree("""
        {
          "data": {
            "type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
            "attributes": {
              "issuedForAgent": "https://ror.org/0566bfb96",
              "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "normalisedPrimarySpecimenObjectId": "https://geocollections.info/specimen/23602",
              "primarySpecimenObjectIdType":"Local",
              "specimenHost": "https://ror.org/0443cwa12"
            }
          }
        }
        """);
  }

  public static JsonNode givenHandleRequest() throws Exception {
    return MAPPER.readTree("""
        {
          "data": [
            {
              "id": "20.5000.1025/V1Z-176-LL4",
              "type": "digitalSpecimen",
              "attributes": {
                "digitalObjectType": "https://doi.org/21.T11148/894b1e6cad57e921764ee",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "normalisedPrimarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "primarySpecimenObjectIdType": "Global",
                "referentName": "Biota",
                "topicDiscipline": "Earth Systems",
                "livingOrPreserved": "Living",
                "markedAsType": true
              }
            }
          ]
        }
        """);
  }

  public static DigitalSpecimen givenAttributes(
      String specimenName, String organisation, Boolean markedAsType,
      boolean addEntityRelationShip) {
    var ds = new DigitalSpecimen()
        .withOdsOrganisationID(organisation)
        .withOdsOrganisationName("National Museum of Natural History")
        .withOdsPhysicalSpecimenIDType(PHYSICAL_SPECIMEN_TYPE)
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
        .withDwcPreparations("")
        .withDctermsModified("2022-11-01T09:59:24.000Z");
    if (addEntityRelationShip) {
      ds.withOdsHasEntityRelationship(
          List.of(new EntityRelationship()
              .withDwcRelationshipOfResource("hasDigitalSpecimen")
              .withDwcRelatedResourceID(SPECIMEN_BASE_URL + HANDLE)
              .withDwcRelationshipEstablishedDate(new Date())));
    }
    return ds;
  }

  public static DigitalSpecimen givenAttributesPlusIdentifier(String specimenName,
      String organisation, Boolean markedAsType) {
    return givenAttributes(specimenName, organisation, markedAsType, false)
        .withOdsHasIdentifier(List.of(
            new Identifier().withDctermsTitle("Specimen label").withDctermsIdentifier(HANDLE)));
  }

  public static AutoAcceptedAnnotation givenAutoAcceptedAnnotation(
      AnnotationProcessingRequest annotation) {
    return new AutoAcceptedAnnotation(new Agent()
        .withType(Type.AS_APPLICATION)
        .withId(APP_HANDLE)
        .withSchemaName(APP_NAME), annotation);
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
        .withDctermsCreator(new Agent()
            .withType(Type.AS_APPLICATION)
            .withId(SOURCE_SYSTEM_ID)
            .withSchemaName(SOURCE_SYSTEM_NAME))
        .withOaHasTarget(new AnnotationTarget()
            .withId(DOI_PREFIX + HANDLE)
            .withOdsID(DOI_PREFIX + HANDLE)
            .withType(TYPE)
            .withOdsType("ods:DigitalSpecimen")
            .withOaHasSelector(new OaHasSelector()
                .withAdditionalProperty("@type", "ods:ClassSelector")
                .withAdditionalProperty("ods:class", "$")));

  }

  public static JsonNode givenJsonPatch() throws JsonProcessingException {
    return MAPPER.readTree(
        "[{\"op\":\"replace\",\"path\":\"/ods:specimenName\",\"value\":\"Biota\"}]");
  }

  public static Map<String, String> givenMediaPidResponse(){
    return Map.of(MEDIA_URL, MEDIA_PID);
  }

}
