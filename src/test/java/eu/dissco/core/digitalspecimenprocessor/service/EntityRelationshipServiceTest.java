package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType.HAS_MEDIA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MIDS_LEVEL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEmptyMediaProcessResult;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEntityRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidProcessResultMedia;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.PidProcessResult;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityRelationshipServiceTest {

  private EntityRelationshipService entityRelationshipService;

  @BeforeEach
  void setUp() {
    entityRelationshipService = new EntityRelationshipService();
  }

  @Test
  void testGetMediaErNullMediaEr() {
    //Given
    var specimen = givenAttributes(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, false, false, false);
    specimen.setOdsHasEntityRelationships(List.of(givenEntityRelationship(null, HAS_MEDIA.getRelationshipName())));
    var currentRecord = new DigitalSpecimenRecord(
        HANDLE,
        MIDS_LEVEL,
        VERSION,
        CREATED,
        new DigitalSpecimenWrapper(
            PHYSICAL_SPECIMEN_ID,
            TYPE,
            specimen,
            ORIGINAL_DATA
        )
    );
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, currentRecord);
    var digitalSpecimen = givenDigitalSpecimenEvent();
    var expected = new MediaRelationshipProcessResult(
        List.of(givenEntityRelationship(null, HAS_MEDIA.getRelationshipName())), List.of(), List.of());

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalMediaRecord()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetMediaErNoMedia() {
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var digitalSpecimen = givenDigitalSpecimenEvent();
    var expected = givenEmptyMediaProcessResult();

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of());

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetMediaErNewEr() {
    //Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var digitalSpecimen = givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID);
    var expected = new MediaRelationshipProcessResult(
        List.of(), List.of(givenDigitalMediaEvent(MEDIA_URL)), List.of());

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of(MEDIA_URL, givenDigitalMediaRecord()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testGetMediaErTombstoneEr() {
    //Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecordWithMediaEr());
    var digitalSpecimen = givenDigitalSpecimenEvent();
    var expected = new MediaRelationshipProcessResult(
        List.of(givenEntityRelationship(MEDIA_PID,
            HAS_MEDIA.getRelationshipName())), List.of(), List.of());

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of(MEDIA_URL, givenDigitalMediaRecord()));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testFindNewSpecimenRelationshipsForMedia() {
    // Given

    // When
    var result = entityRelationshipService.findNewSpecimenRelationshipsForMedia(
        givenDigitalMediaRecord(), givenPidProcessResultMedia());

    // Then
    assertThat(result).isEmpty();
  }

  @Test
  void testFindNewSpecimenRelationshipsForMediaNoNewSpecimens() {
    // Given

    // When
    var result = entityRelationshipService.findNewSpecimenRelationshipsForMedia(
        givenDigitalMediaRecord(),
        new PidProcessResult(MEDIA_PID, Set.of(HANDLE, SECOND_HANDLE)));

    // Then
    assertThat(result).isEqualTo(Set.of(SECOND_HANDLE));
  }

}
