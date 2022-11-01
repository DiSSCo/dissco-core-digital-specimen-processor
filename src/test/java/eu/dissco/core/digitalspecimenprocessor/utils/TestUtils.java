package eu.dissco.core.digitalspecimenprocessor.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import java.time.Instant;
import java.util.List;

public class TestUtils {

  public static ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();
  public static String HANDLE = "20.5000.1025/V1Z-176-LL4";
  public static int MIDS_LEVEL = 1;
  public static int VERSION = 1;
  public static Instant CREATED = Instant.parse("2022-11-01T09:59:24.00Z");
  public static String AAS = "OCR";
  public static String TYPE = "GeologyRockSpecimen";
  public static String PHYSICAL_SPECIMEN_ID = "https://geocollections.info/specimen/23602";
  public static String PHYSICAL_SPECIMEN_TYPE = "cetaf";
  public static String SPECIMEN_NAME = "Biota";
  public static String ORGANIZATION_ID = "https://ror.org/0443cwa12";
  public static String DATASET_ID = null;
  public static String PHYSICAL_SPECIMEN_COLLECTION = null;
  public static String SOURCE_SYSTEM_ID = "20.5000.1025/MN0-5XP-FFD";
  public static JsonNode DATA = generateSpecimenData();
  public static JsonNode ORIGINAL_DATA = generateSpecimenOriginalData();
  public static String DWCA_ID = null;

  private static JsonNode generateSpecimenOriginalData() {
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

  private static JsonNode generateSpecimenData() {
    try {
      return MAPPER.readTree(
          """
              {
                "abcd:unitID": "152-4972",
                "abcd:sourceID": "GIT",
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
    return new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        givenDigitalSpecimen("Another SpecimenName")
    );
  }

  public static DigitalSpecimenEvent givenDigitalSpecimenEvent() {
    return new DigitalSpecimenEvent(
        List.of(AAS),
        givenDigitalSpecimen()
    );
  }

  public static DigitalSpecimen givenDigitalSpecimen() {
    return givenDigitalSpecimen(SPECIMEN_NAME);
  }

  public static DigitalSpecimen givenDigitalSpecimen(String specimenName) {
    return new DigitalSpecimen(
        TYPE,
        PHYSICAL_SPECIMEN_ID,
        PHYSICAL_SPECIMEN_TYPE,
        specimenName,
        ORGANIZATION_ID,
        DATASET_ID,
        PHYSICAL_SPECIMEN_COLLECTION,
        SOURCE_SYSTEM_ID,
        DATA,
        ORIGINAL_DATA,
        DWCA_ID
    );
  }

}
