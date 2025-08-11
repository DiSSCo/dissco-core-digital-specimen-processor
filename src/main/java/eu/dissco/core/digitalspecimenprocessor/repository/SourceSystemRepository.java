package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.SOURCE_SYSTEM;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.SourceSystemMass;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoJsonBMappingException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class SourceSystemRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public Map<String, SourceSystemMass> getSourceSystemMass(Set<String> sourceSystemId) {
    return context.select(SOURCE_SYSTEM.asterisk())
        .from(SOURCE_SYSTEM)
        .where(SOURCE_SYSTEM.ID.in(sourceSystemId))
        .and(SOURCE_SYSTEM.TOMBSTONED.isNull())
        .fetchMap(SOURCE_SYSTEM.ID, this::mapToSourceSystemMass);
  }

  private SourceSystemMass mapToSourceSystemMass(Record dbRecord) {
    try {
      var sourceSystem = mapper.readTree(dbRecord.get(SOURCE_SYSTEM.DATA).data());
      List<String> specimenMass =
          sourceSystem.get("ods:specimenMachineAnnotationServices") != null ?
              mapper.convertValue(sourceSystem.get("ods:specimenMachineAnnotationServices"),
                  new TypeReference<>() {
                  }) :
              List.of();
      List<String> mediaMass = sourceSystem.get("ods:mediaMachineAnnotationServices") != null ?
          mapper.convertValue(sourceSystem.get("ods:mediaMachineAnnotationServices"),
              new TypeReference<>() {
              }) :
          List.of();
      return new SourceSystemMass(specimenMass, mediaMass);
    } catch (JsonProcessingException e) {
      log.error("Unable to read source system {}", dbRecord.get(SOURCE_SYSTEM.ID), e);
      throw new DisscoJsonBMappingException(
          "Unable to read source system " + dbRecord.get(SOURCE_SYSTEM.ID), e);
    }
  }
}
