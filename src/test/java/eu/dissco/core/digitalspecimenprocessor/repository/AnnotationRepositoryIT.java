package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.ANNOTATION;
import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.ANNOTATION_ID_2;
import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.ANNOTATION_ID_3;
import static eu.dissco.core.digitalspecimenprocessor.utils.AnnotationTestUtils.givenAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.THIRD_HANDLE;
import static org.assertj.core.api.Assertions.assertThat;

import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.AnnotationStatusEnum;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OaMotivation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.jooq.JSONB;
import org.jooq.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AnnotationRepositoryIT extends BaseRepositoryIT {

  private AnnotationRepository annotationRepository;

  @BeforeEach
  void setup() {
    annotationRepository = new AnnotationRepository(context, MAPPER);
  }

  @AfterEach
  void destroy() {
    context.truncate(ANNOTATION).execute();
  }

  @Test
  void testGetAcceptedAnnotations() {
    // Given
    var annotation1 = givenAnnotation();
    var annotation2 = givenAnnotation(OaMotivation.OA_EDITING, true, HANDLE_PREFIX + ANNOTATION_ID_2, DOI_PREFIX + HANDLE);
    var annotation3 = givenAnnotation(HANDLE_PREFIX + ANNOTATION_ID_3, DOI_PREFIX + SECOND_HANDLE);
    var annotation4 = givenAnnotation(OaMotivation.OA_EDITING, true, HANDLE_PREFIX + THIRD_HANDLE, DOI_PREFIX + HANDLE);
    var expected = Map.of(
        HANDLE, List.of(annotation1, annotation2),
        SECOND_HANDLE, List.of(annotation3)
    );
    var annotations = Map.of(
        annotation1, AnnotationStatusEnum.ACCEPTED,
        annotation2, AnnotationStatusEnum.ACCEPTED,
        annotation3, AnnotationStatusEnum.ACCEPTED,
        annotation4, AnnotationStatusEnum.MERGED
    );
    postAnnotations(annotations);
    var targetIds = Set.of(DOI_PREFIX + HANDLE, DOI_PREFIX + SECOND_HANDLE);

    // When
    var result = annotationRepository.getAcceptedAnnotationsForObject(targetIds);

    // Then
    assertThat(result.keySet()).isEqualTo(expected.keySet());
    for (var entry : result.entrySet()){
      assertThat(entry.getValue()).hasSameElementsAs(expected.get(entry.getKey()));
    }
  }

  private void postAnnotations(Map<Annotation, AnnotationStatusEnum> annotations)  {
    List<Query> queryList = new ArrayList<>();
    for (var entry : annotations.entrySet()) {
      var annotation = entry.getKey();
      var query = context.insertInto(ANNOTATION)
          .set(ANNOTATION.ID, annotation.getId().replace(HANDLE_PREFIX, ""))
          .set(ANNOTATION.VERSION, annotation.getOdsVersion())
          .set(ANNOTATION.TYPE, annotation.getOdsFdoType())
          .set(ANNOTATION.MOTIVATION, annotation.getOaMotivation().value())
          .set(ANNOTATION.MJR_JOB_ID, annotation.getOdsJobID())
          .set(ANNOTATION.BATCH_ID, annotation.getOdsBatchID())
          .set(ANNOTATION.CREATOR, annotation.getDctermsCreator().getId())
          .set(ANNOTATION.CREATED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.MODIFIED, annotation.getDctermsModified().toInstant())
          .set(ANNOTATION.LAST_CHECKED, annotation.getDctermsCreated().toInstant())
          .set(ANNOTATION.TARGET_ID, annotation.getOaHasTarget().getId())
          .set(ANNOTATION.DATA, JSONB.jsonb(MAPPER.writeValueAsString(annotation)))
          .set(ANNOTATION.ANNOTATION_STATUS, entry.getValue());
      queryList.add(query);
    }
    context.batch(queryList).execute();
  }

}
