package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.repository.DigitalMediaRepository;
import java.util.List;
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

  /*
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
        HANDLE, new MediaRelationshipProcessResult(List.of(),
            List.of(firstMediaEventA, firstMediaEventB)),
        SECOND_HANDLE,
        new MediaRelationshipProcessResult(List.of(), List.of(secondMediaEvent))
    );
    given(mediaRepository.getDigitalMediaUrisFromIdKey(List.of())).willReturn(Map.of());

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
        HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()),
        SECOND_HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents);

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromIdKey(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsException() {
    // Given
    var firstRecord = givenDigitalSpecimenRecord();
    var secondRecord = givenDifferentUnequalSpecimen(SECOND_HANDLE,
        PHYSICAL_SPECIMEN_ID_ALT);
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, firstRecord, PHYSICAL_SPECIMEN_ID_ALT,
        secondRecord);
    var mediaEvents = List.of(givenDigitalMediaEvent(),
        givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID_ALT, MEDIA_URL_ALT));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When / Then
    assertThrows(IllegalStateException.class,
        () -> mediaService.getExistingDigitalMedia(currentSpecimens, mediaEvents));
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
        HANDLE, new MediaRelationshipProcessResult(
            firstRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()),
        SECOND_HANDLE, new MediaRelationshipProcessResult(
            secondRecord.digitalSpecimenWrapper().attributes().getOdsHasEntityRelationships(),
            List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of(
        new DigitalMediaKey(HANDLE, MEDIA_URL), MEDIA_PID,
        new DigitalMediaKey(SECOND_HANDLE, MEDIA_URL_ALT), MEDIA_PID_ALT));

    // When
    var result = mediaService.getExistingDigitalMedia(currentSpecimens, Collections.emptyList());

    // Then
    assertThat(expected).isEqualTo(result);
    then(mediaRepository).should().getDigitalMediaUrisFromIdKey(mediaPidsCaptor.capture());
    assertThat(mediaPidsCaptor.getValue()).containsExactlyInAnyOrder(MEDIA_PID, MEDIA_PID_ALT);
  }

  @Test
  void testGetExistingMediaRelationshipsNoMedia() {
    // Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var expected = Map.of(HANDLE, new MediaRelationshipProcessResult(List.of(), List.of()));
    given(mediaRepository.getDigitalMediaUrisFromIdKey(anyList())).willReturn(Map.of());

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
        new MediaRelationshipProcessResult(tombstonedEr, List.of())
    );

    // When
    mediaService.removeSpecimenRelationshipsFromMedia(Set.of(updatedSpecimenRecord));

    // Then
    then(mediaRepository).should().removeSpecimenRelationshipsFromMedia(List.of(MEDIA_PID));
  }*/

}
