package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ANOTHER_SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_ENRICHMENT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.UPDATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.UPDATED_STR;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMedia;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperNoOriginalData;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalMedia;
import static org.assertj.core.api.Assertions.assertThat;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.Option;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaWrapper;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen.OdsTopicDiscipline;
import eu.dissco.core.digitalspecimenprocessor.schema.EntityRelationship;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Slf4j
class EqualityServiceTest {

  EqualityService equalityService;
  Configuration jsonPathConfiguration = Configuration.builder()
      .options(Option.AS_PATH_LIST, Option.SUPPRESS_EXCEPTIONS, Option.ALWAYS_RETURN_LIST)
      .build();

  @BeforeEach
  void setup() {
    equalityService = new EqualityService(jsonPathConfiguration, MAPPER);
  }

  @ParameterizedTest
  @MethodSource("provideEqualSpecimens")
  void testEqualSpecimens(DigitalSpecimenWrapper currentDigitalSpecimen,
      DigitalSpecimenWrapper digitalSpecimen, MediaRelationshipProcessResult mediaProcessResult) {

    // When
    var result = equalityService.specimensAreEqual(currentDigitalSpecimen, digitalSpecimen,
        mediaProcessResult);

    // Then
    assertThat(result).isTrue();
  }

  @ParameterizedTest
  @MethodSource("provideEqualMedia")
  void testEqualMedia(DigitalMediaRecord currentDigitalMedia,
      DigitalMediaWrapper digitalMediaWrapper) {
    // Given

    // When
    var result = equalityService.mediaAreEqual(currentDigitalMedia, digitalMediaWrapper,
        Set.of());

    // Then
    assertThat(result).isTrue();
  }

  @Test
  void testUnequalSpecimens() {
    // Given
    var currentDigitalSpecimen = givenDigitalSpecimenWrapper();
    var digitalSpecimen = new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, ANOTHER_SPECIMEN_NAME,
        new DigitalSpecimen().withOdsTopicDiscipline(OdsTopicDiscipline.ECOLOGY), null);

    // When
    var result = equalityService.specimensAreEqual(currentDigitalSpecimen, digitalSpecimen,
        givenEmptyMediaProcessResult());

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testUnequalMedia() {
    // Given
    var wrapper = new DigitalMediaWrapper(
        TYPE_MEDIA,
        givenUnequalDigitalMedia(MEDIA_URL),
        MAPPER.createObjectNode()
    );

    // When
    var result = equalityService.mediaAreEqual(givenDigitalMediaRecord(), wrapper, Set.of());

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testUnequalMediaNewSpecimen() {
    // Given

    // When
    var result = equalityService.mediaAreEqual(givenDigitalMediaRecord(),
        givenDigitalMediaWrapper(), Set.of(HANDLE));

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testNewMediaRelationship() {
    // Given
    var currentDigitalSpecimen = givenDigitalSpecimenWrapper();
    var digitalSpecimen = givenDigitalSpecimenWrapper();
    var mediaProcessResult = new MediaRelationshipProcessResult(List.of(),
        List.of(new DigitalMediaEvent(null, null)));

    // When
    var result = equalityService.specimensAreEqual(currentDigitalSpecimen, digitalSpecimen,
        mediaProcessResult);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testTombstonedMediaRelationship() {
    // Given
    var currentDigitalSpecimen = givenDigitalSpecimenWrapper();
    var digitalSpecimen = givenDigitalSpecimenWrapper();
    var mediaProcessResult = new MediaRelationshipProcessResult(
        List.of(new EntityRelationship()), List.of());

    // When
    var result = equalityService.specimensAreEqual(currentDigitalSpecimen, digitalSpecimen,
        mediaProcessResult);

    // Then
    assertThat(result).isFalse();
  }

  @Test
  void testSetEntityRelationshipDate() {
    // Given
    var currentDigitalSpecimen = givenDigitalSpecimenWrapperWithMediaEr(PHYSICAL_SPECIMEN_ID, true);
    var digitalSpecimenWrapper = changeTimestamps(givenDigitalSpecimenWrapper(true, true));
    var digitalSpecimenEvent = new DigitalSpecimenEvent(
        List.of(MAS),
        digitalSpecimenWrapper,
        List.of(givenDigitalMediaEvent())
    );
    var targetEr = currentDigitalSpecimen.attributes().getOdsHasEntityRelationships().stream()
        .filter(entityRelationship -> !entityRelationship.getDwcRelationshipOfResource()
            .equals(HAS_MEDIA.getName()))
        .toList();

    var attributes = givenDigitalSpecimenWrapper(true)
        .attributes()
        .withDctermsCreated(null)
        .withOdsIsKnownToContainMedia(true)
        .withDctermsModified(UPDATED_STR)
        .withOdsHasEntityRelationships(
            targetEr
        );
    var expected = new DigitalSpecimenEvent(
        List.of(MAS),
        new DigitalSpecimenWrapper(
            PHYSICAL_SPECIMEN_ID, TYPE, attributes, ORIGINAL_DATA
        ),
        List.of(givenDigitalMediaEvent())
    );

    // When
    var result = equalityService.setEventDatesSpecimen(currentDigitalSpecimen,
        digitalSpecimenEvent);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testSetEntityRelationshipDateMedia() {
    // Given
    var entityRelationship = givenEntityRelationship(ORGANISATION_ID, "hasRor");
    var expectedWrapper = new DigitalMediaWrapper(
        TYPE_MEDIA,
        givenDigitalMedia(MEDIA_URL).withOdsHasEntityRelationships(List.of(entityRelationship)),
        MAPPER.createObjectNode()
    );
    var expected = new DigitalMediaEvent(
        List.of(MEDIA_ENRICHMENT),
        expectedWrapper);
    var event = new DigitalMediaEvent(
        List.of(MEDIA_ENRICHMENT),
        changeTimestamps(expectedWrapper));
    var currentRecord = new DigitalMediaRecord(
        MEDIA_PID,
        MEDIA_URL,
        VERSION,
        CREATED,
        List.of(MEDIA_ENRICHMENT),
        givenDigitalMedia(MEDIA_URL).withOdsHasEntityRelationships(List.of(entityRelationship)),
        MAPPER.createObjectNode()
    );

    // When
    var result = equalityService.setEventDatesMedia(currentRecord, event);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> provideEqualSpecimens() {
    return Stream.of(
        Arguments.of(givenDigitalSpecimenWrapper(),
            changeTimestamps(givenDigitalSpecimenWrapper()), givenEmptyMediaProcessResult()),
        Arguments.of(givenDigitalSpecimenWrapper(true, true),
            changeTimestamps(givenDigitalSpecimenWrapper(true, true)),
            givenEmptyMediaProcessResult()),
        Arguments.of(givenDigitalSpecimenWrapperWithMediaEr(PHYSICAL_SPECIMEN_ID, true),
            changeTimestamps(givenDigitalSpecimenWrapper(true, true)),
            givenEmptyMediaProcessResult()),
        Arguments.of(givenDigitalSpecimenWrapper(),
            changeTimestamps(givenDigitalSpecimenWrapperNoOriginalData()),
            givenEmptyMediaProcessResult()));
  }

  private static Stream<Arguments> provideEqualMedia() {
    return Stream.of(
        Arguments.of(givenDigitalMediaRecord(), givenDigitalMediaWrapper()),
        Arguments.of(givenDigitalMediaRecord(), changeTimestamps(givenDigitalMediaWrapper()))
    );
  }

  private static DigitalSpecimenWrapper changeTimestamps(
      DigitalSpecimenWrapper digitalSpecimenWrapper) {
    var attributes = digitalSpecimenWrapper.attributes();
    attributes
        .withDctermsCreated(UPDATED)
        .withDctermsModified(UPDATED_STR);
    if (attributes.getOdsHasEntityRelationships() != null) {
      attributes.getOdsHasEntityRelationships()
          .forEach(er -> er.setDwcRelationshipEstablishedDate(UPDATED));
    }
    return new DigitalSpecimenWrapper(digitalSpecimenWrapper.physicalSpecimenID(),
        digitalSpecimenWrapper.type(), attributes, digitalSpecimenWrapper.originalAttributes());

  }

  private static DigitalMediaWrapper changeTimestamps(
      DigitalMediaWrapper digitalMediaWrapper) {
    var attributes = digitalMediaWrapper.attributes();
    attributes
        .withDctermsCreated(UPDATED);
    if (attributes.getOdsHasEntityRelationships() != null) {
      attributes.getOdsHasEntityRelationships()
          .forEach(er -> er.setDwcRelationshipEstablishedDate(UPDATED));
    }
    return new DigitalMediaWrapper(digitalMediaWrapper.type(),
        attributes, digitalMediaWrapper.originalAttributes());

  }


}
