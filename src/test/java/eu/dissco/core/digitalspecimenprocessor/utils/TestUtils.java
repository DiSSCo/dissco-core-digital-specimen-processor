package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.HandleAttribute;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.core.io.ClassPathResource;

public class TestUtils {

  public static ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  public static String HANDLE = "20.5000.1025/V1Z-176-LL4";
  public static String SECOND_HANDLE = "20.5000.1025/XXX-XXX-XXX";
  public static String THIRD_HANDLE = "20.5000.1025/YYY-YYY-YYY";
  public static int MIDS_LEVEL = 1;
  public static int VERSION = 1;
  public static Instant CREATED = Instant.parse("2022-11-01T09:59:24.000Z");
  public static String AAS = "OCR";
  public static String TYPE = "GeologyRockSpecimen";
  public static String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
  public static String PHYSICAL_SPECIMEN_TYPE = "cetaf";
  public static String SPECIMEN_NAME = "Biota";
  public static String ANOTHER_SPECIMEN_NAME = "Another SpecimenName";
  public static String ORGANISATION_ID = "https://ror.org/0443cwa12";
  public static String ANOTHER_ORGANISATION = "Another organisation";
  public static String DATASET_ID = null;
  public static String PHYSICAL_SPECIMEN_COLLECTION = null;
  public static String SOURCE_SYSTEM_ID = "20.5000.1025/MN0-5XP-FFD";
  public static JsonNode ORIGINAL_DATA = generateSpecimenOriginalData();
  public static String DWCA_ID = null;
  public static String TOPIC_DISCIPLINE = "Earth Systems";
  public static final String GENERATED_HANDLE = "20.5000.1025/YYY-YYY-YYY";

  public static final byte[] LOCAL_OBJECT_ID = " 002b51e5-b8e1-4b2b-a841-86c34dca9ef6:040ck2b86".getBytes(StandardCharsets.UTF_8);

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
        givenDigitalSpecimen()
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
        givenDigitalSpecimen(PHYSICAL_SPECIMEN_ID, specimenName, organisation)
    );
  }

  public static DigitalSpecimenRecord givenDifferentUnequalSpecimen(String handle,
      String physicalIdentifier) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimen(physicalIdentifier, ANOTHER_SPECIMEN_NAME, ANOTHER_ORGANISATION));
  }

  public static DigitalSpecimenRecord givenDigitalSpecimenRecord(String handle,
      String physicalSpecimenId) {
    return new DigitalSpecimenRecord(
        handle,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimen(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID)
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent(String physicalSpecimenId) {
    return new DigitalSpecimenEvent(
        List.of(AAS),
        givenDigitalSpecimen(physicalSpecimenId, SPECIMEN_NAME, ORGANISATION_ID)
    );
  }


  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return new DigitalSpecimenEvent(
        List.of(AAS),
        givenDigitalSpecimen()
    );
  }

  public static DigitalSpecimen givenDigitalSpecimen() {
    return givenDigitalSpecimen(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, ORGANISATION_ID);
  }

  public static DigitalSpecimen givenDigitalSpecimen(String physicalSpecimenId, String specimenName,
      String organisation) {
    return new DigitalSpecimen(
        physicalSpecimenId,
        TYPE,
        givenAttributes(specimenName, organisation),
        ORIGINAL_DATA
    );
  }

  public static JsonNode givenAttributes(String specimenName, String organisation) {
    var attributes = new ObjectMapper().createObjectNode();
    attributes.put("ods:physicalSpecimenIdType", PHYSICAL_SPECIMEN_TYPE);
    attributes.put("ods:organisationId", organisation);
    attributes.put("ods:specimenName", specimenName);
    attributes.put("ods:datasetId", DATASET_ID);
    attributes.put("ods:physicalSpecimenCollection", PHYSICAL_SPECIMEN_COLLECTION);
    attributes.put("ods:sourceSystemId", SOURCE_SYSTEM_ID);
    attributes.put("dwca:id", DWCA_ID);
    attributes.put("dcterms:license", "http://creativecommons.org/licenses/by-nc/4.0/");
    attributes.put("ods:objectType", "");
    attributes.put("ods:modified","2017-09-26T12:27:21.000+00:00");
    return attributes;
  }

  public static Map<String, String> givenHandleComponentResponse(){
    return givenHandleComponentResponse(List.of(PHYSICAL_SPECIMEN_ID), List.of(HANDLE));
  }

  public static Map<String, String> givenHandleComponentResponse(List<String> physIds, List<String> handles){
    assert(physIds.size()==handles.size());
    Map<String, String> pidMap = new HashMap<>();
    for (int i = 0; i<physIds.size(); i++){
      pidMap.put(physIds.get(i), handles.get(i));
    }
    return pidMap;
  }

  public static String loadResourceFile(String fileName) throws IOException {
    return new String(new ClassPathResource(fileName).getInputStream()
            .readAllBytes(), StandardCharsets.UTF_8);
  }

  public static Map<String, String> givenHandleComponentResponse(List<DigitalSpecimenRecord> records){
    Map<String, String> response = new HashMap<>();
    for(var specimenRecord : records){
      response.put(specimenRecord.digitalSpecimen().physicalSpecimenId(), specimenRecord.id());
    }
    return response;
  }

}
