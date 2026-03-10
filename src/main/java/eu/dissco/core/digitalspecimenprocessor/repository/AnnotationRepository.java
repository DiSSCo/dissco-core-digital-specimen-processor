package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.ANNOTATION;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.AnnotationStatusEnum;
import eu.dissco.core.digitalspecimenprocessor.exception.DisscoJsonBMappingException;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import java.time.Instant;
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
public class AnnotationRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  public Map<String, List<Annotation>> getAcceptedAnnotationsForObject(Set<String> targetIdsWithProxy){
    return context.select(ANNOTATION.asterisk())
        .from(ANNOTATION)
        .where(ANNOTATION.TARGET_ID.in(targetIdsWithProxy))
        .and(ANNOTATION.ANNOTATION_STATUS.eq(AnnotationStatusEnum.ACCEPTED))
        .fetchGroups(AnnotationRepository::stripProxyFromTargetId, this::mapToAnnotation);
  }

  public void markAnnotationsAsMerged(Set<String> annotationIds){
    context.update(ANNOTATION)
        .set(ANNOTATION.LAST_CHECKED, Instant.now())
        .set(ANNOTATION.ANNOTATION_STATUS, AnnotationStatusEnum.MERGED)
        .where(ANNOTATION.ID.in(annotationIds))
        .execute();
  }

  private static String stripProxyFromTargetId(Record dbRecord){
    return dbRecord.get(ANNOTATION.TARGET_ID).replace(DOI_PROXY, "");
  }

  private Annotation mapToAnnotation(Record dbRecord){
    try {
      return mapper.readValue(dbRecord.get(ANNOTATION.DATA).data(),
          Annotation.class);
    } catch (JsonProcessingException e) {
      log.error("Failed to get data from database, Unable to parse JSONB to JSON", e);
      throw new DisscoJsonBMappingException("Unable to convert jsonb to annotation", e);
    }
  }

}
