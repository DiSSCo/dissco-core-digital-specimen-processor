package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MidsServiceTest {

  private MidsService midsService;

  @BeforeEach
  void setup() {
    midsService = new MidsService();
  }

  @ParameterizedTest
  @MethodSource("provideDigitalSpecimen")
  void testCalculateMids(DigitalSpecimen digitalSpecimen, int midsLevel) {
    // Given

    // When
    var result = midsService.calculateMids(digitalSpecimen);

    // Then
    assertThat(result).isEqualTo(midsLevel);
  }

  private static Stream<Arguments> provideDigitalSpecimen() throws JsonProcessingException {
    return Stream.of(
        Arguments.of(TestUtils.givenDigitalSpecimen(), 0),
        Arguments.of(MAPPER.readValue(MISSING_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(WHITE_SPACE_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(NULL_STRING_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(MIDS_1, DigitalSpecimen.class), 1),
        Arguments.of(MAPPER.readValue(MISSING_LONGITUDE, DigitalSpecimen.class), 1),
        Arguments.of(MAPPER.readValue(MISSING_QUALITATIVE_LOCATION, DigitalSpecimen.class), 1),
        Arguments.of(MAPPER.readValue(UNKNOWN_SPECIMEN_TYPE, DigitalSpecimen.class), 1),
        Arguments.of(MAPPER.readValue(BIO_LACKS_MEDIA, DigitalSpecimen.class), 1),
        Arguments.of(MAPPER.readValue(BIO_MIDS_2, DigitalSpecimen.class), 2),
        Arguments.of(MAPPER.readValue(MIDS_2, DigitalSpecimen.class), 2));
  }


  private final static String MISSING_LONGITUDE = """
      {
        "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
        "ods:type": "GeologyRockSpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "cetaf",
          "ods:organizationId": "https://ror.org/0443cwa12",
          "ods:specimenName": "Biota",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:earliestAgeOrLowestStage": "Pakerort Stage",
          "dwc:latestAgeOrHighestStage": "Pakerort Stage",
          "dwc:country": "Estonia",
          "dwc:countryCode": "EE",
          "dwc:decimalLatitude": "59.465625",
          "dwc:decimalLongitude": null
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String BIO_MIDS_2 = """
      {
        "ods:physicalSpecimenId": "HBG500000:00g30e956",
        "ods:type": "BotanySpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "combined",
          "ods:organizationId": "https://ror.org/00g30e956",
          "ods:specimenName": "Calanthe crenulata",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "https://creativecommons.org/licenses/by/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:country": "Indonesia",
          "dwc:countryCode": "ID",
          "dwc:decimalLatitude": "-4.066340",
          "dwc:decimalLongitude": "104.096684",
          "ods:collectingNumber": "612",
          "ods:collector": "Winkler, H. (Hans) K.A.",
          "ods:dateCollected": "1924-12-10",
          "ods:hasMedia": "true",
          "dwc:typeStatus": "Known not a type"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String BIO_LACKS_MEDIA = """
      {
        "ods:physicalSpecimenId": "HBG500000:00g30e956",
        "ods:type": "BotanySpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "combined",
          "ods:organizationId": "https://ror.org/00g30e956",
          "ods:specimenName": "Calanthe crenulata",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "https://creativecommons.org/licenses/by/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:country": "Indonesia",
          "dwc:countryCode": "ID",
          "dwc:decimalLatitude": "-4.066340",
          "dwc:decimalLongitude": "104.096684",
          "ods:collectingNumber": "612",
          "ods:collector": "Winkler, H. (Hans) K.A.",
          "ods:dateCollected": "1924-12-10",
          "ods:hasMedia": "false",
          "dwc:typeStatus": "Known not a type"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String UNKNOWN_SPECIMEN_TYPE = """
      {
        "ods:physicalSpecimenId": "HBG500000:00g30e956",
        "ods:type": "UnknownType",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "combined",
          "ods:organizationId": "https://ror.org/00g30e956",
          "ods:specimenName": "Calanthe crenulata",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "https://creativecommons.org/licenses/by/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:country": "Indonesia",
          "dwc:countryCode": "ID",
          "dwc:decimalLatitude": "-4.066340",
          "dwc:decimalLongitude": "104.096684",
          "ods:collectingNumber": "612",
          "ods:collector": "Winkler, H. (Hans) K.A.",
          "ods:dateCollected": "1924-12-10",
          "ods:hasMedia": "true",
          "dwc:typeStatus": "Known not a type"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String MISSING_QUALITATIVE_LOCATION = """
      {
        "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
        "ods:type": "GeologyRockSpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "cetaf",
          "ods:organizationId": "https://ror.org/0443cwa12",
          "ods:specimenName": "Biota",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:earliestAgeOrLowestStage": "Pakerort Stage",
          "dwc:latestAgeOrHighestStage": "Pakerort Stage",
          "dwc:decimalLatitude": "59.465625",
          "dwc:decimalLongitude": "25.059035"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String MIDS_2 = """
      {
        "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
        "ods:type": "GeologyRockSpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "cetaf",
          "ods:organizationId": "https://ror.org/0443cwa12",
          "ods:specimenName": "Biota",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00",
          "dwc:earliestAgeOrLowestStage": "Pakerort Stage",
          "dwc:latestAgeOrHighestStage": "Pakerort Stage",
          "dwc:country": "Estonia",
          "dwc:countryCode": "EE",
          "dwc:decimalLatitude": "59.465625",
          "dwc:decimalLongitude": "25.059035",
          "dwc:typeStatus": "Known not a type"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String MIDS_1 = """
      {
        "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
        "ods:type": "GeologyRockSpecimen",
        "ods:attributes": {
          "ods:physicalSpecimenIdType": "cetaf",
          "ods:organizationId": "https://ror.org/0443cwa12",
          "ods:specimenName": "Biota",
          "ods:datasetId": null,
          "ods:physicalSpecimenCollection": null,
          "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
          "dwca:id": null,
          "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
          "ods:objectType": "single specimen",
          "dcterms:modified": "2017-09-26T12:27:21.000+00:00"
        },
        "ods:originalAttributes": {}
      }
      """;

  private final static String MISSING_VALUE =
      """
            {
            "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
            "ods:type": "GeologyRockSpecimen",
            "ods:attributes": {
              "ods:physicalSpecimenIdType": "cetaf",
              "ods:organizationId": "https://ror.org/0443cwa12",
              "ods:specimenName": "Biota",
              "ods:datasetId": null,
              "ods:physicalSpecimenCollection": null,
              "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
              "dwca:id": null,
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
              "dcterms:modified": "2017-09-26T12:27:21.000+00:00"
            },
            "ods:originalAttributes": {}
          }
          """;

  private final static String WHITE_SPACE_VALUE =
      """
            {
            "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
            "ods:type": "GeologyRockSpecimen",
            "ods:attributes": {
              "ods:physicalSpecimenIdType": "cetaf",
              "ods:organizationId": "https://ror.org/0443cwa12",
              "ods:specimenName": "Biota",
              "ods:datasetId": null,
              "ods:physicalSpecimenCollection": null,
              "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
              "dwca:id": null,
              "ods:objectType": "single specimen",
              "dcterms:license": "    ",
              "dcterms:modified": "2017-09-26T12:27:21.000+00:00"
            },
            "ods:originalAttributes": {}
          }
          """;

  private final static String NULL_STRING_VALUE =
      """
            {
            "ods:physicalSpecimenId": "https://geocollections.info/specimen/23602",
            "ods:type": "GeologyRockSpecimen",
            "ods:attributes": {
              "ods:physicalSpecimenIdType": "cetaf",
              "ods:organizationId": "https://ror.org/0443cwa12",
              "ods:specimenName": "null",
              "ods:datasetId": null,
              "ods:physicalSpecimenCollection": null,
              "ods:sourceSystemId": "20.5000.1025/MN0-5XP-FFD",
              "dwca:id": null,
              "ods:objectType": "single specimen",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
              "dcterms:modified": "2017-09-26T12:27:21.000+00:00"
            },
            "ods:originalAttributes": {}
          }
          """;
}
