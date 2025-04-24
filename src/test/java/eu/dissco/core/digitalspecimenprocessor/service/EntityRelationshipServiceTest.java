package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecordWithMediaEr;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenEntityRelationship;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.domain.EntityRelationshipType;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.MediaRelationshipProcessResult;
import java.util.List;
import java.util.Map;
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
  void testGetMediaErNoMedia() {
    //Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var digitalSpecimen = givenDigitalSpecimenEvent();
    var expected = new MediaRelationshipProcessResult(
        List.of(), List.of());

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of());

    // Then
    assertThat(expected).isEqualTo(result);
  }


  @Test
  void testGetMediaErNewEr() {
    //Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecord());
    var digitalSpecimen = givenDigitalSpecimenEvent(PHYSICAL_SPECIMEN_ID);
    var expected = new MediaRelationshipProcessResult(
        List.of(), List.of(givenDigitalMediaEvent(PHYSICAL_SPECIMEN_ID)));

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of(MEDIA_URL, givenDigitalMediaRecord()));

    // Then
    assertThat(expected).isEqualTo(result);
  }

  @Test
  void testGetMediaErTombstoneEr() {
    //Given
    var currentSpecimens = Map.of(PHYSICAL_SPECIMEN_ID, givenDigitalSpecimenRecordWithMediaEr());
    var digitalSpecimen = givenDigitalSpecimenEvent();
    var expected = new MediaRelationshipProcessResult(
        List.of(givenEntityRelationship(MEDIA_PID, EntityRelationshipType.HAS_MEDIA)), List.of());

    // When
    var result = entityRelationshipService.processMediaRelationshipsForSpecimen(
        currentSpecimens, digitalSpecimen, Map.of(MEDIA_URL, givenDigitalMediaRecord()));

    // Then
    assertThat(expected).isEqualTo(result);
  }

}
