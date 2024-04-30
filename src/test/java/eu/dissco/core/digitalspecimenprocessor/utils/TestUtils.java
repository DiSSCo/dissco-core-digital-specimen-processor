package eu.dissco.core.digitalspecimenprocessor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectEventWithoutDoi;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectWithoutDoi;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaObjectWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalEntity;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsLivingOrPreserved;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsPhysicalSpecimenIdType;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationships;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifiers;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUtils {

  public static ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  public static String HANDLE = "20.5000.1025/V1Z-176-LL4";
  public static String SECOND_HANDLE = "20.5000.1025/XXX-XXX-XXX";
  public static String THIRD_HANDLE = "20.5000.1025/YYY-YYY-YYY";
  public static int MIDS_LEVEL = 1;
  public static int VERSION = 1;
  public static Instant CREATED = Instant.parse("2022-11-01T09:59:24.000Z");
  public static String MAS = "OCR";
  public static String TYPE = "GeologyRockSpecimen";
  public static String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
  public static OdsPhysicalSpecimenIdType PHYSICAL_SPECIMEN_TYPE = OdsPhysicalSpecimenIdType.GLOBAL;
  public static String SPECIMEN_NAME = "Biota";
  public static String ANOTHER_SPECIMEN_NAME = "Another SpecimenName";
  public static String ORGANISATION_ID = "https://ror.org/0443cwa12";
  public static String ANOTHER_ORGANISATION = "Another organisation";
  public static String DATASET_ID = null;
  public static String PHYSICAL_SPECIMEN_COLLECTION = null;
  public static String SOURCE_SYSTEM_ID = "20.5000.1025/MN0-5XP-FFD";
  public static JsonNode ORIGINAL_DATA = generateSpecimenOriginalData();
  public static OdsTopicDiscipline TOPIC_DISCIPLINE = OdsTopicDiscipline.BOTANY;

  public static String SPECIMEN_BASE_URL = "https://doi.org/";

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
        givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, specimenName, organisation)
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
            ANOTHER_ORGANISATION));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle,
      String physicalSpecimenId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID)
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID),
        List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent())
    );
  }


  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return givenDigitalSpecimenEvent(false);
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(boolean hasMedia) {
    return new DigitalSpecimenEvent(
        List.of(MAS),
        givenDigitalSpecimenWrapper(),
        hasMedia ? List.of(givenDigitalMediaEvent(), givenDigitalMediaEvent()) : List.of()
    );
  }

  private static DigitalMediaObjectEventWithoutDoi givenDigitalMediaEvent() {
    return new DigitalMediaObjectEventWithoutDoi(
        List.of("image-metadata"),
        new DigitalMediaObjectWithoutDoi(
            "StillImage",
            PHYSICAL_SPECIMEN_ID,
            new DigitalEntity().withAcAccessUri("https://an-image.org"),
            MAPPER.createObjectNode()
        )
    );
  }

  public static DigitalMediaObjectEvent givenDigitalMediaEventWithRelationship() {
    return new DigitalMediaObjectEvent(
        List.of("image-metadata"),
        new DigitalMediaObjectWrapper(
            "StillImage",
            HANDLE,
            new DigitalEntity().withAcAccessUri("https://an-image.org").withEntityRelationships(
                List.of(new EntityRelationships().withEntityRelationshipType("hasDigitalSpecimen")
                    .withObjectEntityIri(SPECIMEN_BASE_URL + "20.5000.1025/V1Z-176-LL4"))
            ),
            MAPPER.createObjectNode()
        ));
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper() {
    return givenDigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID);
  }

  public static DigitalSpecimenWrapper givenDigitalSpecimenWrapper(String physicalSpecimenId,
      String specimenName,
      String organisation) {
    return new DigitalSpecimenWrapper(
        physicalSpecimenId,
        TYPE,
        givenAttributes(specimenName, organisation, true),
        ORIGINAL_DATA
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
      response.put(specimenRecord.digitalSpecimenWrapper().physicalSpecimenId(),
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
              "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
              "attributes": {
                "fdoProfile": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
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
            "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
            "attributes": {
              "fdoProfile": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
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
            "type": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
            "attributes": {
              "fdoProfile": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764",
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
                "fdoProfile": "https://hdl.handle.net/21.T11148/d8de0819e144e4096645",
                "digitalObjectType": "https://hdl.handle.net/21.T11148/894b1e6cad57e921764e",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
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
      String specimenName, String organisation, Boolean markedAsType) {
    return new DigitalSpecimen()
        .withDwcInstitutionId(organisation)
        .withDwcInstitutionName("National Museum of Natural History")
        .withOdsPhysicalSpecimenIdType(PHYSICAL_SPECIMEN_TYPE)
        .withOdsNormalisedPhysicalSpecimenId(PHYSICAL_SPECIMEN_ID)
        .withOdsSpecimenName(specimenName)
        .withOdsTopicDiscipline(TOPIC_DISCIPLINE)
        .withOdsSourceSystem(SOURCE_SYSTEM_ID)
        .withOdsLivingOrPreserved(OdsLivingOrPreserved.PRESERVED)
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/")
        .withDwcCollectionId(PHYSICAL_SPECIMEN_COLLECTION)
        .withDwcDatasetName(DATASET_ID)
        .withOdsMarkedAsType(markedAsType)
        .withDwcPreparations("")
        .withDctermsModified("2017-09-26T12:27:21.000+00:00");
  }

  public static DigitalSpecimen givenAttributesPlusIdentifier(String specimenName, String organisation, Boolean markedAsType){
    return givenAttributes(specimenName, organisation, markedAsType)
        .withIdentifiers(List.of(new Identifiers("Specimen label", HANDLE, false, null, true)));
  }

}
