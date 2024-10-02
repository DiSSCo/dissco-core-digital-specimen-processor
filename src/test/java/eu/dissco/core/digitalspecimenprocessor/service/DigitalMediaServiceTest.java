package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapperWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DigitalMediaServiceTest {

  private DigitalMediaService mediaService;

  @Mock
  private DigitalMediaRepository mediaRepository;

  @BeforeEach
  void setup() {
    mediaService = new DigitalMediaService(mediaRepository);
  }

  @Test
  void testGetExistingMediaRelationshipsUnchanged() {
    // Given
    var incomingMediaEvent = givenDigitalMediaEvent();
    var currentEntityRelationships = givenDigitalSpecimenWrapperWithMediaEr(PHYSICAL_SPECIMEN_ID,
        true).attributes()
        .getOdsHasEntityRelationship();
    var currentMediaRelationships = currentEntityRelationships.stream().filter(
            entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
                .equals(HAS_MEDIA.getName()))
        .toList();
    var expected = new DigitalMediaProcessResult(currentMediaRelationships, List.of(), List.of());
    given(mediaRepository.getDigitalMediaUrisFromId(List.of(MEDIA_PID))).willReturn(
        givenMediaPidResponse());

    // When
    var result = mediaService.getExistingDigitalMedia(currentEntityRelationships,
        List.of(incomingMediaEvent));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetExistingMediaRelationshipsNewMedia() {
    // Given
    var incomingMediaEvent = givenDigitalMediaEvent();
    var currentEntityRelationships = givenDigitalSpecimenWrapper(true).attributes()
        .getOdsHasEntityRelationship();
    var expected = new DigitalMediaProcessResult(List.of(), List.of(), List.of(incomingMediaEvent));
    given(mediaRepository.getDigitalMediaUrisFromId(List.of())).willReturn(Map.of());

    // When
    var result = mediaService.getExistingDigitalMedia(currentEntityRelationships,
        List.of(incomingMediaEvent));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetExistingMediaRelationshipsTombstoneMedia() {
    // Given
    var currentEntityRelationships = givenDigitalSpecimenWrapperWithMediaEr(PHYSICAL_SPECIMEN_ID,
        false).attributes()
        .getOdsHasEntityRelationship();
    var expected = new DigitalMediaProcessResult(List.of(), currentEntityRelationships, List.of());
    given(mediaRepository.getDigitalMediaUrisFromId(List.of(MEDIA_PID))).willReturn(
        givenMediaPidResponse());

    // When
    var result = mediaService.getExistingDigitalMedia(currentEntityRelationships,
        List.of());

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetExistingMediaNoMedia() {
    // Given
    var currentEntityRelationships = givenDigitalSpecimenWrapper(true).attributes()
        .getOdsHasEntityRelationship();
    var expected = givenEmptyMediaProcessResult();

    // When
    var result = mediaService.getExistingDigitalMedia(currentEntityRelationships,
        List.of());

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testRemoveSpecimenRelationshipsFromMedia() {
    // Given
    var currentDigitalSpecimenRecord = givenDigitalSpecimenRecordWithMediaEr(HANDLE,
        PHYSICAL_SPECIMEN_ID, true);
    var tombstonedEr = currentDigitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationship().stream().filter(
            entityRelationship -> entityRelationship.getDwcRelationshipOfResource()
                .equals(HAS_MEDIA.getName()))
        .toList();

    var updatedSpecimenRecord = new UpdatedDigitalSpecimenRecord(
        givenDigitalSpecimenRecord(),
        List.of(),
        currentDigitalSpecimenRecord,
        MAPPER.createObjectNode(),
        List.of(),
        new DigitalMediaProcessResult(List.of(), tombstonedEr, List.of())
    );

    // When
    mediaService.removeSpecimenRelationshipsFromMedia(Set.of(updatedSpecimenRecord));

    // Then
    then(mediaRepository).should().removeSpecimenRelationshipsFromMedia(List.of(MEDIA_PID));
  }

  @Test
  void testRemoveSpecimenRelationshipsFromMediaNoMedia() {
    // Given
    var updatedSpecimenRecord = new UpdatedDigitalSpecimenRecord(
        givenDigitalSpecimenRecord(),
        List.of(),
        givenDigitalSpecimenRecord(),
        MAPPER.createObjectNode(),
        List.of(),
        givenEmptyMediaProcessResult()
    );

    //When
    mediaService.removeSpecimenRelationshipsFromMedia(Set.of(updatedSpecimenRecord));

    // Then
    then(mediaRepository).shouldHaveNoInteractions();
  }

}
