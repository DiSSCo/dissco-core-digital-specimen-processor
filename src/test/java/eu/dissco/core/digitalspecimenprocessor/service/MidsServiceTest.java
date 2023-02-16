package eu.dissco.core.digitalspecimenprocessor.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

  private static final ObjectMapper MAPPER = new ObjectMapper().findAndRegisterModules();

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
    return Stream.of(Arguments.of(TestUtils.givenDigitalSpecimen(), 0),
        Arguments.of(MAPPER.readValue(MISSING_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(WHITE_SPACE_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(NULL_STRING_VALUE, DigitalSpecimen.class), 0),
        Arguments.of(MAPPER.readValue(MIDS_1, DigitalSpecimen.class), 1));
  }

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
          "ods:modified": "2017-09-26T12:27:21.000+00:00"
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
              "ods:modified": "2017-09-26T12:27:21.000+00:00"
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
              "ods:modified": "2017-09-26T12:27:21.000+00:00"
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
              "ods:modified": "2017-09-26T12:27:21.000+00:00"
            },
            "ods:originalAttributes": {}
          }
          """;
}
