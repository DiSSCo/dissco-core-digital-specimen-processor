package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static org.mockito.BDDMockito.then;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaConsumerServiceTest {

  @Mock
  private ProcessingService processingService;
  @Mock
  private KafkaPublisherService publisherService;

  private KafkaConsumerService service;

  @BeforeEach
  void setup() {
    service = new KafkaConsumerService(MAPPER, processingService, publisherService);
  }

  @Test
  void testGetMessages() {
    // Given
    var message = givenMessage();

    // When
    service.getMessages(List.of(message));

    // Then
    then(processingService).should().handleMessages(List.of(givenDigitalSpecimenEvent()));
  }

  @Test
  void testGetInvalidMessages() {
    // Given
    var message = givenInvalidMessage();

    // When
    service.getMessages(List.of(message));

    // Then
    then(processingService).should().handleMessages(List.of());

  }

  private String givenInvalidMessage() {
    return """
        {
          "enrichmentList": [
            "OCR"
          ],
          "digitalSpecimen": {
            "type": "GeologyRockSpecimen",
            "physicalSpecimenId": "https://geocollections.info/specimen/23602",
            "physicalSpecimenIdType": "global",
            "specimenName": "Biota",
            "organisationId": "https://ror.org/0443cwa12",
            "datasetId": null,
            "physicalSpecimenCollection": null,
            "sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
            "data": {},
            "originalData": {},
            "dwcaId": null
          }
        }""";
  }

  private String givenMessage() {
    return """
        {
          "enrichmentList": [
            "OCR"
            ],
          "digitalSpecimen": {
            "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
            "ods:type": "GeologyRockSpecimen",
            "ods:attributes": {
              "ods:physicalSpecimenIdType": "GLOBAL",
              "dwc:institutionId": "https://ror.org/0443cwa12",
              "dwc:institutionName": "National Museum of Natural History",
              "ods:specimenName": "Biota",
              "dwc:datasetName": null,
              "dwc:collectionId": null,
              "ods:sourceSystem": "20.5000.1025/MN0-5XP-FFD",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
              "dwc:preparations": "",
              "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
              "ods:topicDiscipline": "Botany",
              "dwc:typeStatus": "holotype",
              "ods:livingOrPreserved": "Preserved"
            },
            "ods:originalAttributes": {
                "abcd:unitID": "152-4972",
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
              }
          },
          "digitalMediaObjectEvents": []
        }""";
  }


}
