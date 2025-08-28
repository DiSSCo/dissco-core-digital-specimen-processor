package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type.PROV_SOFTWARE_AGENT;
import static eu.dissco.core.digitalspecimenprocessor.util.AgentUtils.createMachineAgent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.VERSION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenWrapper;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatchSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.AgentRoleType;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Identifier.DctermsType;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsChangeValue;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProvenanceServiceTest {

  @Mock
  private ApplicationProperties properties;

  private ProvenanceService service;

  private static List<Agent> givenExpectedAgents() {
    return List.of(
        createMachineAgent(SOURCE_SYSTEM_NAME, SOURCE_SYSTEM_ID, SOURCE_SYSTEM,
            DctermsType.HANDLE, PROV_SOFTWARE_AGENT),
        createMachineAgent(APP_NAME, APP_HANDLE, AgentRoleType.PROCESSING_SERVICE,
            DctermsType.DOI, PROV_SOFTWARE_AGENT)
    );
  }

  @BeforeEach
  void setup() {
    this.service = new ProvenanceService(MAPPER, properties);
  }

  @Test
  void testGenerateCreateEventSpecimen() {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var digitalSpecimen = new DigitalSpecimenRecord(HANDLE, 2, 1, CREATED,
        givenDigitalSpecimenWrapper(true, false), Set.of(), false, List.of());

    // When
    var event = service.generateCreateEventSpecimen(digitalSpecimen);

    // Then
    assertThat(event.getDctermsIdentifier()).isEqualTo(
        DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isNull();
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasAgents()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateUpdateEventSpecimen() throws JsonProcessingException {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var anotherDigitalSpecimen = givenUnequalDigitalSpecimenRecord();

    // When
    var event = service.generateUpdateEventSpecimen(anotherDigitalSpecimen, givenJsonPatchSpecimen());

    // Then
    assertThat(event.getDctermsIdentifier()).isEqualTo(
        DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasAgents()).isEqualTo(givenExpectedAgents());
  }

  List<OdsChangeValue> givenChangeValue() {
    return List.of(
        new OdsChangeValue()
            .withAdditionalProperty("op", "replace")
            .withAdditionalProperty("path", "/ods:specimenName")
            .withAdditionalProperty("value", "Biota")
    );
  }
}
