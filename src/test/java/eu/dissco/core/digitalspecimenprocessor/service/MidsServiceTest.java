package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.GeologicalContext;
import eu.dissco.core.digitalspecimenprocessor.schema.Georeference;
import eu.dissco.core.digitalspecimenprocessor.schema.Location;
import eu.dissco.core.digitalspecimenprocessor.schema.Occurrences;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import java.util.List;
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

  private static Stream<Arguments> provideDigitalSpecimen() {
    return Stream.of(
        Arguments.of(TestUtils.givenDigitalSpecimen(), 0),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue(null),
            MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("      "),
                MAPPER.createObjectNode()), 0),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("null"),
            MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("in alcohol"),
                MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingLongitude(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingCountry(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenInvalidType(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenBotanySpecimen(Boolean.FALSE),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenBotanySpecimen(Boolean.TRUE),
                MAPPER.createObjectNode()), 2),
        Arguments.of(new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, TYPE, givenFullPaleoSpecimen(),
            MAPPER.createObjectNode()), 2));
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenFullPaleoSpecimen() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withDwcTypeStatus("holotype")
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOccurrences(List.of(new Occurrences().withLocation(
            new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withGeoreference(new Georeference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withGeologicalContext(new GeologicalContext().withDwcGroup("Group")))));
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenBotanySpecimen(
      Boolean hasMedia) {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withDwcTypeStatus("holotype")
        .withOccurrences(List.of(
            new Occurrences()
                .withDwcEventDate("22-09-2023")
                .withDwcFieldNumber("A field number")
                .withLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withGeoreference(new Georeference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsHasMedia(hasMedia);
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenInvalidType() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsTopicDiscipline(OdsTopicDiscipline.UNCLASSIFIED);
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenMissingCountry() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withDwcTypeStatus("holotype")
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOccurrences(List.of(new Occurrences().withLocation(
            new Location()
                .withGeoreference(new Georeference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withGeologicalContext(new GeologicalContext().withDwcEarliestEonOrLowestEonothem("Archean")))));
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenPreparationValue(
      String preparationValue) {
    return baseDigitalSpecimen()
        .withDwcPreparations(preparationValue)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY);
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen baseDigitalSpecimen() {
    return new eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen()
        .withOdsPhysicalSpecimenIdType(PHYSICAL_SPECIMEN_TYPE)
        .withDwcInstitutionId(ORGANISATION_ID)
        .withOdsSpecimenName(SPECIMEN_NAME)
        .withOdsSourceSystem(SOURCE_SYSTEM_ID)
        .withDctermsModified("2017-09-26T12:27:21.000+00:00")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/");
  }

  private static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenMissingLongitude() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withDwcTypeStatus("holotype")
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOccurrences(List.of(new Occurrences().withLocation(
            new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                .withGeoreference(new Georeference().withDwcDecimalLatitude(59.465625))
                .withGeologicalContext(new GeologicalContext().withDwcGroup("Group")))));
  }

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
}
