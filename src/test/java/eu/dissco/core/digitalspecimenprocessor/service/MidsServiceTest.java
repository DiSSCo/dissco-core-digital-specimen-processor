package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsGeologicalContext;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsGeoReference;
import eu.dissco.core.digitalspecimenprocessor.schema.Identification;
import eu.dissco.core.digitalspecimenprocessor.schema.Location;
import eu.dissco.core.digitalspecimenprocessor.schema.Event;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsHasTaxonIdentification;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import java.util.ArrayList;
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
        Arguments.of(TestUtils.givenDigitalSpecimenWrapper(), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue(null),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("      "),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenPreparationValue("null"),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingTopic(),
                MAPPER.createObjectNode()), 0),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenPreparationValue("in alcohol"),
                MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingLongitude(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingCountry(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenInvalidType(),
            MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(Boolean.FALSE),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(null),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimenMissingFieldNumber(),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMissingLocation(),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(List.of()),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(null),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanyNoOccurrenceSpecimen(new ArrayList<>() {
                  {
                    add(null);
                  }
                }),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(new Location().withOdsGeoReference(new OdsGeoReference())),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(new Location().withOdsGeoReference(null)),
                MAPPER.createObjectNode()), 1),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimen(Boolean.TRUE),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenBotanySpecimenAlternative(),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenFullPaleoSpecimen(),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenFullBioSpecimenMissingTax(true, false),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenFullBioSpecimenMissingTax(false, true),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenMissingUncertaintyLocation()),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenMissingPercisionLocation()),
                MAPPER.createObjectNode()), 2),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenLocationId()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE, givenMidsThreeBioSpecimen(),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenFootprintLocation()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenVerbatimCoordinateLocation()),
                MAPPER.createObjectNode()), 3),
        Arguments.of(
            new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE,
                givenMidsThreeGenericSpecimen(givenVerbatimLatLongLocation()),
                MAPPER.createObjectNode()), 3)
    );
  }

  private static DigitalSpecimen givenFullBioSpecimenMissingTax(boolean missingName,
      boolean missingIdentificationId) {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withDwcRecordedByID("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withOdsHasIdentification(List.of(
            new Identification().withDwcIdentifiedByID(missingIdentificationId ? null : "ID1")
                .withOdsHasTaxonIdentification(
                    List.of(new OdsHasTaxonIdentification().withDwcScientificNameID(
                        missingName ? null : "ID2")))))
        .withOdsIsKnownToContainMedia(true)
        .withOdsIsMarkedAsType(false)
        .withOdsHasEvent(List.of(new Event()
            .withDwcVerbatimEventDate("A verbatim date as is on the label")
            .withDwcFieldNumber("1202")
            .withOdsLocation(new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withOdsGeoReference(new OdsGeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035)
                    .withDwcGeodeticDatum("WGS84")
                    .withDwcCoordinateUncertaintyInMeters(10.0)
                    .withDwcCoordinatePrecision(0.0001))
                .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group")))));
  }

  private static DigitalSpecimen givenBotanySpecimenMissingFieldNumber() {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsIsMarkedAsType(false)
        .withOdsHasEvent(List.of(
            new Event()
                .withDwcYear(2023)
                .withOdsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withOdsGeoReference(new OdsGeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsIsKnownToContainMedia(true);
  }

  private static DigitalSpecimen givenBotanyNoOccurrenceSpecimen(List<Event> events) {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsIsMarkedAsType(true)
        .withOdsHasEvent(events)
        .withOdsIsKnownToContainMedia(true);
  }

  private static DigitalSpecimen givenBotanySpecimenAlternative() {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsIsMarkedAsType(false)
        .withOdsHasEvent(List.of(
            new Event()
                .withDwcYear(2023)
                .withDwcRecordNumber("A record number")
                .withOdsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withOdsGeoReference(new OdsGeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsIsKnownToContainMedia(true);
  }

  private static DigitalSpecimen givenMissingLocation() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(true)
        .withDwcRecordedByID("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOdsHasIdentification(List.of(new Identification().withDwcIdentificationID("ID1")
            .withOdsHasTaxonIdentification(
                List.of(new OdsHasTaxonIdentification().withDwcScientificNameID("ID2")))))
        .withOdsHasEvent(List.of(new Event()));
  }

  private static DigitalSpecimen givenMidsThreeBioSpecimen() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(true)
        .withDwcRecordedByID("ORCID")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withOdsHasIdentification(List.of(new Identification().withDwcIdentifiedByID("ID1")
            .withOdsHasTaxonIdentification(
                List.of(new OdsHasTaxonIdentification().withDwcScientificNameID("ID2")))))
        .withOdsIsKnownToContainMedia(true)
        .withOdsIsMarkedAsType(false)
        .withOdsHasEvent(List.of(new Event()
            .withDwcVerbatimEventDate("A verbatim date as is on the label")
            .withDwcFieldNumber("1202")
            .withOdsLocation(new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withOdsGeoReference(new OdsGeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035)
                    .withDwcGeodeticDatum("WGS84")
                    .withDwcCoordinateUncertaintyInMeters(10.0)
                    .withDwcCoordinatePrecision(0.0001))
                .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group")))));
  }

  private static Location givenLocationId() {
    return new Location()
        .withDwcCountry("Estonia")
        .withDwcCountryCode("EE")
        .withDwcLocationID("LOC_ID")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenFootprintLocation() {
    return new Location()
        .withDwcCountry("Estonia")
        .withDwcCountryCode("EE")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcFootprintSRS("Some footprint SRS, WGS84 for example")
            .withDwcFootprintWKT("POINT (12.559220 55.702230)"))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }


  private static Location givenVerbatimCoordinateLocation() {
    return new Location()
        .withDwcLocationID("LOC_ID")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcVerbatimCoordinates("59.465625, 25.059035"))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenVerbatimLatLongLocation() {
    return new Location()
        .withDwcLocationID("LOC_ID")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcVerbatimLatitude("59.465625")
            .withDwcVerbatimLongitude("25.059035"))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenMissingUncertaintyLocation() {
    return new Location()
        .withDwcCountry("The Netherlands")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcGeodeticDatum("WGS84"))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }

  private static Location givenMissingPercisionLocation() {
    return new Location()
        .withDwcCountry("The Netherlands")
        .withOdsGeoReference(new OdsGeoReference()
            .withDwcDecimalLatitude(59.465625)
            .withDwcDecimalLongitude(25.059035)
            .withDwcGeodeticDatum("WGS84")
            .withDwcCoordinateUncertaintyInMeters(100.00))
        .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group"));
  }

  private static DigitalSpecimen givenMidsThreeGenericSpecimen(Location location) {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOdsHasIdentification(List.of(new Identification().withDwcIdentifiedByID("ID1")
            .withOdsHasTaxonIdentification(
                List.of(new OdsHasTaxonIdentification().withDwcScientificNameID("ID2")))))
        .withOdsHasEvent(List.of(new Event().withOdsLocation(location)));
  }

  private static DigitalSpecimen givenFullPaleoSpecimen() {
    return baseDigitalSpecimen().withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(false)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOdsHasEvent(List.of(new Event().withOdsLocation(
            new Location()
                .withDwcCountry("Estonia")
                .withDwcCountryCode("EE")
                .withOdsGeoReference(new OdsGeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group")))));
  }

  private static DigitalSpecimen givenBotanySpecimen(Boolean hasMedia) {
    return baseDigitalSpecimen().withDwcPreparations("in alcohol")
        .withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
        .withDwcRecordedBy("sam Leeflang")
        .withOdsIsMarkedAsType(true)
        .withOdsHasEvent(List.of(
            new Event()
                .withDwcEventDate("22-09-2023")
                .withDwcFieldNumber("A field number")
                .withOdsLocation(
                    new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                        .withOdsGeoReference(new OdsGeoReference()
                            .withDwcDecimalLatitude(59.465625)
                            .withDwcDecimalLongitude(25.059035)))))
        .withOdsIsKnownToContainMedia(hasMedia);
  }

  private static DigitalSpecimen givenInvalidType() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsTopicDiscipline(OdsTopicDiscipline.UNCLASSIFIED);
  }

  private static DigitalSpecimen givenMissingCountry() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOdsHasEvent(List.of(new Event().withOdsLocation(
            new Location()
                .withOdsGeoReference(new OdsGeoReference()
                    .withDwcDecimalLatitude(59.465625)
                    .withDwcDecimalLongitude(25.059035))
                .withOdsGeologicalContext(
                    new OdsGeologicalContext().withDwcEarliestEonOrLowestEonothem("Archean")))));
  }

  private static DigitalSpecimen givenMissingTopic() {
    return baseDigitalSpecimen()
        .withDwcPreparations("alcohol")
        .withDctermsModified("22-05-2024")
        .withDctermsLicense("A license")
        .withOdsSpecimenName("A nice name")
        .withOdsTopicDiscipline(null);
  }

  private static DigitalSpecimen givenPreparationValue(
      String preparationValue) {
    return baseDigitalSpecimen()
        .withDwcPreparations(preparationValue)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY);
  }

  private static DigitalSpecimen baseDigitalSpecimen() {
    return new DigitalSpecimen()
        .withOdsPhysicalSpecimenIDType(PHYSICAL_SPECIMEN_TYPE)
        .withOdsOrganisationID(ORGANISATION_ID)
        .withOdsSpecimenName(SPECIMEN_NAME)
        .withOdsSourceSystemID(SOURCE_SYSTEM_ID)
        .withDctermsModified("2017-09-26T12:27:21.000+00:00")
        .withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/");
  }

  private static DigitalSpecimen givenMissingLongitude() {
    return baseDigitalSpecimen()
        .withDwcPreparations("single specimen")
        .withOdsIsMarkedAsType(true)
        .withOdsTopicDiscipline(OdsTopicDiscipline.PALAEONTOLOGY)
        .withOdsHasEvent(List.of(new Event().withOdsLocation(
            new Location().withDwcCountry("Estonia").withDwcCountryCode("EE")
                .withOdsGeoReference(new OdsGeoReference().withDwcDecimalLatitude(59.465625))
                .withOdsGeologicalContext(new OdsGeologicalContext().withDwcGroup("Group")))));
  }

  @BeforeEach
  void setup() {
    midsService = new MidsService();
  }

  @ParameterizedTest
  @MethodSource("provideDigitalSpecimen")
  void testCalculateMids(DigitalSpecimenWrapper digitalSpecimen, int midsLevel) {
    // Given

    // When
    var result = midsService.calculateMids(digitalSpecimen);

    // Then
    assertThat(result).isEqualTo(midsLevel);
  }
}
