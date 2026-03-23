package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.mas.MasJobRequest;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.domain.relation.DigitalMediaRelationshipTombstoneEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMqProperties;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

@Service
@Slf4j
@AllArgsConstructor
public class RabbitMqPublisherService {

	private static final String MEDIA_ROUTING_KEY_PREFIX = ".digital-media.";

	private static final String SPECIMEN_ROUTING_KEY_PREFIX = ".digital-specimen.";

	private final JsonMapper mapper;

	private final ProvenanceService provenanceService;

	private final RabbitTemplate rabbitTemplate;

	private final RabbitMqProperties rabbitMqProperties;

	public void publishCreateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord) {
		var event = provenanceService.generateCreateEventSpecimen(digitalSpecimenRecord);
		rabbitTemplate.convertAndSend(rabbitMqProperties.getProvenance().getExchangeName(),
				generateRoutingKeySpecimen(digitalSpecimenRecord), mapper.writeValueAsString(event));
	}

	public void publishCreateEventMedia(DigitalMediaRecord digitalMediaRecord) {
		var event = provenanceService.generateCreateEventMedia(digitalMediaRecord);
		rabbitTemplate.convertAndSend(rabbitMqProperties.getProvenance().getExchangeName(),
				generateRoutingKeyMedia(digitalMediaRecord), mapper.writeValueAsString(event));
	}

	public void publishMasJobRequest(MasJobRequest masJobRequest) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getMasScheduler().getExchangeName(),
				rabbitMqProperties.getMasScheduler().getRoutingKeyName(), mapper.writeValueAsString(masJobRequest));
	}

	public void publishUpdateEventSpecimen(DigitalSpecimenRecord digitalSpecimenRecord, JsonNode jsonPatch) {
		var event = provenanceService.generateUpdateEventSpecimen(digitalSpecimenRecord, jsonPatch);
		rabbitTemplate.convertAndSend(rabbitMqProperties.getProvenance().getExchangeName(),
				generateRoutingKeySpecimen(digitalSpecimenRecord), mapper.writeValueAsString(event));
	}

	private String generateRoutingKeySpecimen(DigitalSpecimenRecord digitalSpecimenRecord) {
		return rabbitMqProperties.getProvenance().getRoutingKeyPrefix() + SPECIMEN_ROUTING_KEY_PREFIX
				+ stripSourceSystemId(
						digitalSpecimenRecord.digitalSpecimenWrapper().attributes().getOdsSourceSystemID());
	}

	public void publishUpdateEventMedia(DigitalMediaRecord digitalMediaRecord, JsonNode jsonPatch) {
		var event = provenanceService.generateUpdateEventMedia(digitalMediaRecord, jsonPatch);
		rabbitTemplate.convertAndSend(rabbitMqProperties.getProvenance().getExchangeName(),
				generateRoutingKeyMedia(digitalMediaRecord), mapper.writeValueAsString(event));
	}

	private String generateRoutingKeyMedia(DigitalMediaRecord digitalMediaRecord) {
		return rabbitMqProperties.getProvenance().getRoutingKeyPrefix() + MEDIA_ROUTING_KEY_PREFIX
				+ stripSourceSystemId(digitalMediaRecord.attributes().getOdsSourceSystemID());
	}

	public void republishSpecimenEvent(DigitalSpecimenEvent event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getSpecimen().getExchangeName(),
				rabbitMqProperties.getSpecimen().getRoutingKeyName(), mapper.writeValueAsString(event));
	}

	public void republishMediaEvent(DigitalMediaEvent event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMedia().getExchangeName(),
				rabbitMqProperties.getDigitalMedia().getRoutingKeyName(), mapper.writeValueAsString(event));
	}

	public void deadLetterEventSpecimen(DigitalSpecimenEvent event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getSpecimen().getDlqExchangeName(),
				rabbitMqProperties.getSpecimen().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
	}

	public void deadLetterEventMedia(DigitalMediaEvent event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMedia().getDlqExchangeName(),
				rabbitMqProperties.getDigitalMedia().getDlqRoutingKeyName(), mapper.writeValueAsString(event));
	}

	public void publishAcceptedAnnotation(AutoAcceptedAnnotation annotation) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getAutoAcceptedAnnotation().getExchangeName(),
				rabbitMqProperties.getAutoAcceptedAnnotation().getRoutingKeyName(),
				mapper.writeValueAsString(annotation));
	}

	public void publishDigitalMediaRelationTombstone(DigitalMediaRelationshipTombstoneEvent event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMediaRelationshipTombstone().getExchangeName(),
				rabbitMqProperties.getDigitalMediaRelationshipTombstone().getRoutingKeyName(),
				mapper.writeValueAsString(event));
	}

	public void deadLetterRawDigitalMediaRelationshipTombstone(String event) {
		rabbitTemplate.convertAndSend(rabbitMqProperties.getDigitalMediaRelationshipTombstone().getDlqExchangeName(),
				rabbitMqProperties.getDigitalMediaRelationshipTombstone().getDlqRoutingKeyName(), event);
	}

	private static String stripSourceSystemId(String sourceSystemId) {
		return sourceSystemId.substring(sourceSystemId.lastIndexOf('/') + 1);
	}

}
