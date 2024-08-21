package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.FDO_APP_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.FDO_APP_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.addErToSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaUpdatePidEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWithEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenInterimMediaEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaEntityRelationshipWithId;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaEventWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalMediaWithoutDOI;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.property.FdoProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.net.URI;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import com.nimbusds.jose.util.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MediaEntityRelationshipServiceTest {

  @Mock
  FdoProperties fdoProperties;
  private MockedStatic<Instant> mockedInstant;
  private MockedStatic<Clock> mockedClock;
  private MediaEntityRelationshipService mediaEntityRelationshipService;


  @BeforeEach
  void setup() {
    mediaEntityRelationshipService = new MediaEntityRelationshipService(fdoProperties);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedInstant = mockStatic(Instant.class);
    mockedInstant.when(Instant::now).thenReturn(instant);
    mockedInstant.when(() -> Instant.from(any())).thenReturn(instant);
    mockedInstant.when(() -> Instant.parse(any())).thenReturn(instant);
    mockedClock = mockStatic(Clock.class);
    mockedClock.when(Clock::systemUTC).thenReturn(clock);
  }

  @AfterEach
  void destroy() {
    mockedInstant.close();
    mockedClock.close();
  }

  @Test
  void testAddNewMediaERs() throws Exception {
    // Given
    var original = givenDigitalSpecimenWithEntityRelationship();
    var expected = givenDigitalSpecimenWithEntityRelationship();
    addErToSpecimenRecord(expected, givenInterimMediaEntityRelationship());
    var specimenMap = Map.of(
        original,
        Pair.of(List.of(""), List.of(givenDigitalMediaEvent()))
    );
    given(fdoProperties.getApplicationName()).willReturn(FDO_APP_NAME);
    given(fdoProperties.getApplicationPID()).willReturn(FDO_APP_ID);

    // When
    mediaEntityRelationshipService.addNewMediaERs(specimenMap);

    // Then
    assertThat(original).isEqualTo(expected);
  }

  @Test
  void testUpdateMediaErs() throws Exception {
    // Given
    var currentRecord = givenDigitalSpecimenWithEntityRelationship();
    var updatedRecord = givenDigitalSpecimenWithEntityRelationship();
    addErToSpecimenRecord(currentRecord, givenInterimMediaEntityRelationship());
    addErToSpecimenRecord(updatedRecord, givenMediaEntityRelationshipWithId());
    var expected = List.of(new UpdatedDigitalSpecimenTuple(
        currentRecord,
        new DigitalSpecimenEvent(List.of(), updatedRecord.digitalSpecimenWrapper(), List.of())
    ));

    // When
    var result = mediaEntityRelationshipService.updateMediaERs(List.of(currentRecord),
        List.of(givenDigitalMediaUpdatePidEvent()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testAddMediaERsToSpecimenBadUri() throws Exception {
    // Given
    var badUri = " 'aaa";
    var original = givenDigitalSpecimenWithEntityRelationship();
    var expected = givenDigitalSpecimenWithEntityRelationship();
    addErToSpecimenRecord(expected, givenInterimMediaEntityRelationship()
        .withOdsRelatedResourceURI(null)
        .withDwcRelatedResourceID(badUri));
    var mediaEvent = new DigitalMediaEventWithoutDOI(
        List.of(),
        new DigitalMediaWithoutDOI(
            "StillImage",
            PHYSICAL_SPECIMEN_ID,
            new DigitalMedia().withAcAccessURI(badUri),
            MAPPER.createObjectNode()
        )
    );
    var specimenMap = Map.of(
        original,
        Pair.of(List.of(""), List.of(mediaEvent)));
    given(fdoProperties.getApplicationName()).willReturn(FDO_APP_NAME);
    given(fdoProperties.getApplicationPID()).willReturn(FDO_APP_ID);

    // When
    mediaEntityRelationshipService.addNewMediaERs(specimenMap);

    // Then
    assertThat(original).isEqualTo(expected);
  }

  @Test
  void testGetMediaErsForUpdatedSpecimen() throws Exception {
    // Given
    String altMediaUri="https://new-image.nl";
    var originalMediaEr = givenMediaEntityRelationshipWithId();
    var newMediaEr = givenInterimMediaEntityRelationship()
        .withOdsRelatedResourceURI(new URI(altMediaUri))
        .withDwcRelatedResourceID(altMediaUri);
    var newMediaEvent = new DigitalMediaEventWithoutDOI(
        List.of("image-metadata"),
        new DigitalMediaWithoutDOI(
            "StillImage",
            PHYSICAL_SPECIMEN_ID,
            new DigitalMedia().withAcAccessURI(altMediaUri),
            MAPPER.createObjectNode()
        )
    );
    var originalRecord = givenDigitalSpecimenRecord();
    addErToSpecimenRecord(originalRecord, originalMediaEr);
    var expectedRecord = givenDigitalSpecimenRecord();
    addErToSpecimenRecord(expectedRecord, originalMediaEr);
    addErToSpecimenRecord(expectedRecord, newMediaEr);
    var tmp = givenDigitalSpecimenEvent(true);
    var originalEvent = new DigitalSpecimenEvent(
        tmp.enrichmentList(),
        tmp.digitalSpecimenWrapper(),
        List.of(tmp.digitalMediaEvents().get(0), newMediaEvent)
    );
    var expected = expectedRecord.digitalSpecimenWrapper();
    given(fdoProperties.getApplicationName()).willReturn(FDO_APP_NAME);
    given(fdoProperties.getApplicationPID()).willReturn(FDO_APP_ID);

    // When
    var result = mediaEntityRelationshipService.getMediaErsForUpdatedSpecimen(originalRecord, originalEvent);

    // Then
    assertThat(result).isEqualTo(expected);
  }


}
