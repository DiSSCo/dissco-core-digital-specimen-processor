package eu.dissco.core.digitalspecimenprocessor.utils;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DATASET_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.DOI_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE_PREFIX;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MIDS_LEVEL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORGANISATION_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.ORIGINAL_DATA;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_COLLECTION;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SOURCE_SYSTEM_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.TYPE_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAttributes;

import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenWrapper;
import io.github.dissco.core.annotationlogic.schema.Agent;
import io.github.dissco.core.annotationlogic.schema.Agent.Type;
import io.github.dissco.core.annotationlogic.schema.Annotation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OaMotivation;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsMergingDecisionStatus;
import io.github.dissco.core.annotationlogic.schema.Annotation.OdsStatus;
import io.github.dissco.core.annotationlogic.schema.AnnotationBody;
import io.github.dissco.core.annotationlogic.schema.AnnotationTarget;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsLivingOrPreserved;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsPhysicalSpecimenIDType;
import io.github.dissco.core.annotationlogic.schema.DigitalSpecimen.OdsTopicDiscipline;
import io.github.dissco.core.annotationlogic.schema.OaHasSelector;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

public class AnnotationTestUtils {

	public static final String ANNOTATION_FDO_TYPE = "https://doi.org/21.T11148/cf458ca9ee1d44a5608f";

	public static final String NEW_VALUE = "Some new value!";

	public static final String ORCID = "https://orcid.org/0000-0002-XXXX-XXXX";

	public static final String ANNOTATION_ID = "10.2500.1025/888-999-000";

	public static final String ANNOTATION_ID_2 = "10.2500.1025/555-6666-777";

	public static final String ANNOTATION_ID_3 = "10.2500.1025/222-333-444";

	private AnnotationTestUtils() {
		// Utility class
	}

	public static Annotation givenAnnotation() {
		return givenAnnotation(HANDLE_PREFIX + ANNOTATION_ID, DOI_PREFIX + HANDLE);
	}

	public static Annotation givenAnnotation(String annotationId, String targetId) {
		return givenAnnotation(OaMotivation.ODS_ADDING, true, annotationId, targetId);
	}

	public static Annotation givenAnnotation(OaMotivation motivation, boolean isTermAnnotation, String annotationId,
			String targetId) {
		var target = isTermAnnotation ? givenOaTargetTerm(motivation, targetId)
				: givenAnnotationTargetClass(motivation, null, targetId);
		AnnotationBody body;
		if (motivation.equals(OaMotivation.ODS_DELETING)) {
			body = new AnnotationBody().withOaValue(List.of());
		}
		else {
			body = isTermAnnotation ? givenOaBodyTerm() : givenOaBodyClass();
		}
		return new Annotation().withId(annotationId)
			.withDctermsIdentifier(annotationId)
			.withType("ods:Annotation")
			.withOdsFdoType(ANNOTATION_FDO_TYPE)
			.withOdsVersion(1)
			.withOdsStatus(OdsStatus.ACTIVE)
			.withOdsMergingDecisionStatus(OdsMergingDecisionStatus.APPROVED)
			.withOaHasBody(body)
			.withOaMotivation(motivation)
			.withOaHasTarget(target)
			.withDctermsCreator(givenAgent(Type.PROV_PERSON))
			.withDctermsCreated(Date.from(CREATED))
			.withDctermsIssued(Date.from(CREATED))
			.withDctermsModified(Date.from(CREATED))
			.withAsGenerator(givenAgent(Type.PROV_SOFTWARE_AGENT));
	}

	private static AnnotationBody givenOaBodyTerm() {
		return new AnnotationBody().withType("oa:TextualBody")
			.withOaValue(new ArrayList<>(List.of(NEW_VALUE)))
			.withDctermsReferences("https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
			.withOdsScore(0.99);
	}

	private static AnnotationBody givenOaBodyClass() {
		return new AnnotationBody().withType("oa:TextualBody")
			.withOaValue(new ArrayList<>(List.of("""
					{
					  "dwc:habitat": "marsh"
					}
					""")))
			.withDctermsReferences("https://medialib.naturalis.nl/file/id/ZMA.UROCH.P.1555/format/large")
			.withOdsScore(0.99);
	}

	private static AnnotationTarget givenOaTargetTerm(OaMotivation motivation, String targetId) {
		var path = OaMotivation.ODS_ADDING.equals(motivation) ? "$['dwc:remarks']" : "$['dwc:collectionID']";
		return givenAnnotationTargetTerm(path, targetId);
	}

	public static AnnotationTarget givenAnnotationTargetTerm(String path, String targetId) {
		return new AnnotationTarget().withId(targetId)
			.withType("ods:DigitalSpecimen")
			.withOdsFdoType(ANNOTATION_FDO_TYPE)
			.withDctermsIdentifier(targetId)
			.withOaHasSelector(new OaHasSelector().withAdditionalProperty("@type", "ods:TermSelector")
				.withAdditionalProperty("ods:term", path));
	}

	public static AnnotationTarget givenAnnotationTargetClass(OaMotivation motivation, String path, String targetId) {
		if (path == null) {
			path = OaMotivation.ODS_ADDING.equals(motivation) ? "$['ods:hasEvents'][*]"
					: "$['ods:hasEntityRelationships'][0]";
		}
		return new AnnotationTarget().withId(targetId)
			.withType("ods:DigitalSpecimen")
			.withOdsFdoType(ANNOTATION_FDO_TYPE)
			.withDctermsIdentifier(targetId)
			.withOaHasSelector(new OaHasSelector().withAdditionalProperty("ods:class", path)
				.withAdditionalProperty("@type", "ods:ClassSelector"));
	}

	public static Agent givenAgent(Type type) {
		return new Agent().withSchemaName("Some agent").withId(ORCID).withType(type);
	}

	public static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenAnnotatedSpecimen(int version) {
		return givenAnnotatedSpecimen().withOdsVersion(version).withOdsMidsLevel(MIDS_LEVEL);
	}

	public static eu.dissco.core.digitalspecimenprocessor.schema.DigitalSpecimen givenAnnotatedSpecimen(){
		return givenAttributes(SPECIMEN_NAME, ORGANISATION_ID, true, false, true)
				.withDwcOrganismRemarks(NEW_VALUE);
	}

	public static DigitalSpecimenRecord givenAnnotatedDigitalSpecimenRecord(){
		return new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, 2, CREATED, givenAnnotatedSpecimenWrapper(), Set.of(),
				false, false, List.of());
	}

	public static DigitalSpecimenRecord givenAnnotatedDigitalSpecimenRecord(int version) {
		return new DigitalSpecimenRecord(HANDLE, MIDS_LEVEL, version, CREATED, givenAnnotatedSpecimenWrapper(version),
				Set.of(), false, false, List.of());
	}

	public static DigitalSpecimen givenAnnotatedLibrarySpecimen() {
		return givenAnnotatedLibrarySpecimen(OaMotivation.ODS_ADDING);
	}

	public static DigitalSpecimenWrapper givenAnnotatedSpecimenWrapper(int version) {
		return new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE_PID, givenAnnotatedSpecimen(version),
				ORIGINAL_DATA);
	}

	public static DigitalSpecimenWrapper givenAnnotatedSpecimenWrapper() {
		return new DigitalSpecimenWrapper(PHYSICAL_SPECIMEN_ID, TYPE_PID,
			givenAnnotatedSpecimen(),ORIGINAL_DATA);
	}

	public static DigitalSpecimen givenAnnotatedLibrarySpecimen(OaMotivation motivation) {
		var specimen = new DigitalSpecimen().withOdsOrganisationID(ORGANISATION_ID)
			.withOdsOrganisationName("National Museum of Natural History")
			.withOdsPhysicalSpecimenIDType(OdsPhysicalSpecimenIDType.GLOBAL)
			.withOdsPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
			.withOdsNormalisedPhysicalSpecimenID(PHYSICAL_SPECIMEN_ID)
			.withOdsSpecimenName(SPECIMEN_NAME)
			.withOdsTopicDiscipline(OdsTopicDiscipline.BOTANY)
			.withOdsSourceSystemID(SOURCE_SYSTEM_ID)
			.withOdsSourceSystemName(SOURCE_SYSTEM_NAME)
			.withOdsLivingOrPreserved(OdsLivingOrPreserved.PRESERVED)
			.withDctermsLicense("http://creativecommons.org/licenses/by-nc/4.0/")
			.withDwcCollectionID(PHYSICAL_SPECIMEN_COLLECTION)
			.withDwcDatasetName(DATASET_ID)
			.withOdsIsMarkedAsType(true)
			.withOdsIsKnownToContainMedia(true)
			.withDctermsModified("2022-11-01T09:59:24.000Z");
		if (motivation.equals(OaMotivation.OA_EDITING)) {
			specimen.withDwcCollectionID(NEW_VALUE);
		}
		else {
			specimen.withDwcOrganismRemarks(NEW_VALUE);
		}
		return specimen;
	}

}
