package eu.dissco.core.digitalspecimenprocessor.controller;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.AAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DATASET_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DWCA_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANIZATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_COLLECTION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.generateSpecimenOriginalData;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEventOld;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenOld;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.exception.NoChangesFoundException;
import eu.dissco.core.digitalspecimenprocessor.service.ProcessingService;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
class DigitalSpecimenControllerTest {

  private final ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
  @Mock
  private ProcessingService processingService;

  private DigitalSpecimenController controller;

  @BeforeEach
  void setup() {
    controller = new DigitalSpecimenController(processingService, mapper);
  }

  @Test
  void testDataMapping() throws NoChangesFoundException {
    // Given
    var oldDigitalSpecimen = givenOldDigitalSpecimen();
    var newDigitalSpecimen = givenNewDigitalSpecimen();
    given(processingService.handleMessages(
        List.of(new DigitalSpecimenEvent(List.of(AAS), newDigitalSpecimen)))).willReturn(
        List.of(new DigitalSpecimenRecord(HANDLE, 0, 1,
            CREATED, newDigitalSpecimen)));

    // When
    var result = controller.upsertDigitalSpecimen(
        new DigitalSpecimenEventOld(List.of(AAS), oldDigitalSpecimen));

    // Then
    assertThat(result.getStatusCode()).isEqualTo(HttpStatus.CREATED);
  }

  private DigitalSpecimen givenNewDigitalSpecimen() {
    return new DigitalSpecimen(
        PHYSICAL_SPECIMEN_ID,
        TYPE,
        givenAttributes(),
        generateSpecimenOriginalData()
    );
  }

  private JsonNode givenAttributes() {
    var attributes = mapper.createObjectNode();
    attributes.put("ods:physicalSpecimenIdType", PHYSICAL_SPECIMEN_TYPE);
    attributes.put("ods:organizationId", ORGANIZATION_ID);
    attributes.put("ods:specimenName", SPECIMEN_NAME);
    attributes.put("ods:datasetId", DATASET_ID);
    attributes.put("ods:physicalSpecimenCollection", PHYSICAL_SPECIMEN_COLLECTION);
    attributes.put("ods:sourceSystemId", SOURCE_SYSTEM_ID);
    return attributes;
  }

  private DigitalSpecimenOld givenOldDigitalSpecimen() {
    return new DigitalSpecimenOld(
        TYPE,
        PHYSICAL_SPECIMEN_ID,
        PHYSICAL_SPECIMEN_TYPE,
        SPECIMEN_NAME,
        ORGANIZATION_ID,
        DATASET_ID,
        PHYSICAL_SPECIMEN_COLLECTION,
        SOURCE_SYSTEM_ID,
        null,
        generateSpecimenOriginalData(),
        DWCA_ID
    );
  }
}
