package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.ANNOTATION;
import static eu.dissco.core.digitalspecimenprocessor.util.DigitalObjectUtils.DOI_PROXY;

import eu.dissco.core.digitalspecimenprocessor.database.jooq.enums.AnnotationStatusEnum;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import tools.jackson.databind.json.JsonMapper;

@Repository
@RequiredArgsConstructor
@Slf4j
public class AnnotationRepository {

	private final DSLContext context;

	private final JsonMapper mapper;

	public Map<String, List<Annotation>> getAcceptedAnnotationsForObject(Set<String> targetIdsWithProxy) {
		return context.select(ANNOTATION.asterisk())
			.from(ANNOTATION)
			.where(ANNOTATION.TARGET_ID.in(targetIdsWithProxy))
			.and(ANNOTATION.ANNOTATION_STATUS.eq(AnnotationStatusEnum.ACCEPTED))
			.fetchGroups(AnnotationRepository::stripProxyFromTargetId, this::mapToAnnotation);
	}

	private static String stripProxyFromTargetId(Record dbRecord) {
		return dbRecord.get(ANNOTATION.TARGET_ID).replace(DOI_PROXY, "");
	}

	private Annotation mapToAnnotation(Record dbRecord) {
		return mapper.readValue(dbRecord.get(ANNOTATION.DATA).data(), Annotation.class);
	}

}
