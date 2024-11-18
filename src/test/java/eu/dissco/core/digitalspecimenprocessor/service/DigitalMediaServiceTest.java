package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID_ALT;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDifferentUnequalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.UpdatedDigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class DigitalMediaServiceTest {

  @Captor
  ArgumentCaptor<List<String>> mediaPidsCaptor;
  private DigitalMediaService mediaService;
  @Mock
  private DigitalMediaRepository mediaRepository;

  @BeforeEach
  void setup() {
    mediaService = new DigitalMediaService(mediaRepository);
  }

  @Test
  void testGetExistingDigitalMediaNew() {
    //
    var firstRecord = givenDigitalSpecimenRecord();
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE, PHYSICAL_SPECIMEN_ID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var firstMediaEventA = givenDigitalMediaEvent();
    var firstMediaEventB = givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID, MEDIA_URL_ALT);
    var secondMediaEvent = givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT);
    var expected = Map.of(
        HANDLE, new DigitalMediaProcessResult(List.of(), List.of(),
            List.of(firstMediaEventA, firstMediaEventB)),
        SECOND_HANDLE,
        new DigitalMediaProcessResult(List.of(), List.of(), List.of(secondMediaEvent))
    );
    given(mediaRepository.getDigitalMediaUrisFromId(List.of())).willReturn(Map.of());

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens,
        List.of(firstMediaEventA, firstMediaEventB, secondMediaEvent));

    // Then
    assertThat(expected).isEqualTo(result);
  }

  @Test
  void testGetExistingMediaRelationshipsUnchanged() {
    // Given
    var firstRecord = givenDigitalSpecimenRecordWithMediaEr();
    var secondRecord = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT, true, 1, MEDIA_PID_ALT);
    var secondRecordMediaER = secondRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships().stream()
        .filter(er -> er.getDwcRelationshipOfResource().equals(HAS_MEDIA.getName()))
        .toList();
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var mediaEvents = List.of(givenDigitalMediaEvent(),
        givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT, MEDIA_URL_ALT));
    var expected = Map.of(
        HANDLE, new DigitalMediaProcessResult(firstRecord.digitalSpecimenWrapper().attributes()
            .getOdsHasEntityRelationships(), List.of(), List.of()),
        SECOND_HANDLE, new DigitalMediaProcessResult(secondRecordMediaER, List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromId(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents);

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromId(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsTombstone() {
    // Given
    var firstRecord = givenDigitalSpecimenRecordWithMediaEr();
    var secondRecord = givenDigitalSpecimenRecordWithMediaEr(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT, false, 1, MEDIA_PID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var expected = Map.of(
        HANDLE, new DigitalMediaProcessResult(List.of(),
            firstRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()),
        SECOND_HANDLE, new DigitalMediaProcessResult(List.of(),
            secondRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()));
    given(mediaRepository.getDigitalMediaUrisFromId(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, Collections.emptyList());

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromId(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsNoMedia() {
    // Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var expected = Map.of(HANDLE, new DigitalMediaProcessResult(List.of(), List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromId(anyList())).willReturn(Map.of());

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, List.of());

    // Then
    assertThat(expected).isEqualTo(result);
  }

  @Test
  void testRemoveSpecimenRelationshipsFromMedia() {
    // Given
    var currentDigitalSpecimenRecord = givenDigitalSpecimenRecordWithMediaEr(HANDLE,
        PHYSICAL_SPECIMEN_ID, true);
    var tombstonedEr = currentDigitalSpecimenRecord.digitalSpecimenWrapper().attributes()
        .getOdsHasEntityRelationships().stream().filter(
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

}
