package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.SOURCE_SYSTEM;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.APP_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.TranslatorType;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.SourceSystemMass;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.jooq.JSONB;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SourceSystemRepositoryIT extends BaseRepositoryIT {

  private SourceSystemRepository sourceSystemRepository;
  private static final String SS_ID = SOURCE_SYSTEM_ID.replace(HANDLE_PREFIX, "");

  @BeforeEach
  void setup() {
    sourceSystemRepository = new SourceSystemRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(SOURCE_SYSTEM).execute();
  }

  @ParameterizedTest
  @MethodSource("provideSourceSystemMasData")
  void testGetSourceSystemMas(JsonNode sourceSystemMasData, Map<String, SourceSystemMass> expected)
      throws JsonProcessingException {
    // Given
    postSourceSystem(sourceSystemMasData);

    // When
    var result = sourceSystemRepository.getSourceSystemMass(Set.of(SS_ID));

    // Then
    assertThat(result).isEqualTo(expected);
  }

  private static Stream<Arguments> provideSourceSystemMasData() {
    return Stream.of(
        Arguments.of(MAPPER.createObjectNode(),
            Map.of(SS_ID, new SourceSystemMass(List.of(), List.of()))),
        Arguments.of(MAPPER.createObjectNode().set("ods:specimenMachineAnnotationServices",
                MAPPER.createArrayNode().add(SECOND_HANDLE)),
            Map.of(SS_ID, new SourceSystemMass(List.of(SECOND_HANDLE), List.of()))),
        Arguments.of(MAPPER.createObjectNode()
                .set("ods:specimenMachineAnnotationServices", MAPPER.createArrayNode()),
            Map.of(SS_ID, new SourceSystemMass(List.of(), List.of()))),
        Arguments.of(MAPPER.createObjectNode()
                .set("ods:mediaMachineAnnotationServices", MAPPER.createArrayNode().add(THIRD_HANDLE)),
            Map.of(SS_ID, new SourceSystemMass(List.of(), List.of(THIRD_HANDLE)))));

  }

  private void postSourceSystem(JsonNode sourceSystemMasData) throws JsonProcessingException {
    context.insertInto(SOURCE_SYSTEM).set(SOURCE_SYSTEM.ID, (SS_ID)).set(SOURCE_SYSTEM.VERSION, 1)
        .set(SOURCE_SYSTEM.NAME, "source system name")
        .set(SOURCE_SYSTEM.ENDPOINT, "http://endpoint").set(SOURCE_SYSTEM.CREATOR, APP_HANDLE)
        .set(SOURCE_SYSTEM.CREATED, CREATED).set(SOURCE_SYSTEM.MODIFIED, CREATED)
        .set(SOURCE_SYSTEM.MAPPING_ID, HANDLE_PREFIX + SOURCE_SYSTEM_ID)
        .set(SOURCE_SYSTEM.TRANSLATOR_TYPE, TranslatorType.dwca)
        .set(SOURCE_SYSTEM.DWC_DP_LINK, "https://s3.link")
        .set(SOURCE_SYSTEM.DATA, mapToJSONB(sourceSystemMasData)).execute();
  }

  private JSONB mapToJSONB(JsonNode sourceSystemMasData) throws JsonProcessingException {
    return JSONB.valueOf(MAPPER.writeValueAsString(sourceSystemMasData));
  }


}
