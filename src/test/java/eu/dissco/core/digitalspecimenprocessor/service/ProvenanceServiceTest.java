package eu.dissco.core.digitalspecimenprocessor.service;

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
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

import eu.dissco.core.digitalspecimenprocessor.component.SourceSystemNameComponent;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.ApplicationProperties;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent;
import eu.dissco.core.digitalspecimenprocessor.schema.Agent.Type;
import eu.dissco.core.digitalspecimenprocessor.schema.OdsChangeValue;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ProvenanceServiceTest {

  @Mock
  private ApplicationProperties properties;

  @Mock
  private SourceSystemNameComponent sourceSystemNameComponent;

  private ProvenanceService service;

  private static List<Agent> givenExpectedAgents() {
    return List.of(
        new Agent()
            .withId(SOURCE_SYSTEM_ID)
            .withType(Type.AS_APPLICATION)
            .withSchemaName(SOURCE_SYSTEM_NAME),
        new Agent()
            .withId(APP_HANDLE)
            .withType(Type.AS_APPLICATION)
            .withSchemaName(APP_NAME)
    );
  }

  @BeforeEach
  void setup() {
    this.service = new ProvenanceService(MAPPER, properties, sourceSystemNameComponent);
  }

  @Test
  void testGenerateCreateEvent() {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var digitalSpecimen = new DigitalSpecimenRecord(HANDLE, 2, 1, CREATED,
        givenDigitalSpecimenWrapper(true));
    given(sourceSystemNameComponent.getSourceSystemName(SOURCE_SYSTEM_ID)).willReturn(
        SOURCE_SYSTEM_NAME);

    // When
    var event = service.generateCreateEvent(digitalSpecimen);

    // Then
    assertThat(event.getOdsID()).isEqualTo(DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isNull();
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  @Test
  void testGenerateUpdateEvent() {
    // Given
    given(properties.getName()).willReturn(APP_NAME);
    given(properties.getPid()).willReturn(APP_HANDLE);
    var digitalSpecimen = new DigitalSpecimenRecord(HANDLE, 2, 1, CREATED,
        givenDigitalSpecimenWrapper(true));
    var anotherDigitalSpecimen = givenUnequalDigitalSpecimenRecord();
    given(sourceSystemNameComponent.getSourceSystemName(SOURCE_SYSTEM_ID)).willReturn(
        SOURCE_SYSTEM_NAME);

    // When
    var event = service.generateUpdateEvent(anotherDigitalSpecimen, digitalSpecimen);

    // Then
    assertThat(event.getOdsID()).isEqualTo(DOI_PREFIX + HANDLE + "/" + VERSION);
    assertThat(event.getProvActivity().getOdsChangeValue()).isEqualTo(givenChangeValue());
    assertThat(event.getProvEntity().getProvValue()).isNotNull();
    assertThat(event.getOdsHasProvAgent()).isEqualTo(givenExpectedAgents());
  }

  List<OdsChangeValue> givenChangeValue() {
    return List.of(new OdsChangeValue()
            .withAdditionalProperty("op", "replace")
            .withAdditionalProperty("path", "/ods:midsLevel")
            .withAdditionalProperty("value", 1),
        new OdsChangeValue()
            .withAdditionalProperty("op", "remove")
            .withAdditionalProperty("path", "/ods:hasEntityRelationship/0"),
        new OdsChangeValue()
            .withAdditionalProperty("op", "replace")
            .withAdditionalProperty("path", "/ods:specimenName")
            .withAdditionalProperty("value", "Another SpecimenName")
    );
  }
}
