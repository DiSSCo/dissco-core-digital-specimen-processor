package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;

import eu.dissco.core.digitalspecimenprocessor.client.PidClient;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;

@Component
@RequiredArgsConstructor
@Slf4j
public class PidComponent {

	private final PidClient pidClient;

	private static final String UNEXPECTED_MSG = "Unexpected response from PID API";

	private static final String UNEXPECTED_LOG = "Unexpected response from PID API. Error: {}. Response: {}";

	public Map<String, String> postPid(List<JsonNode> request, boolean isSpecimen) throws PidException {
		var responseJsonNode = pidClient.postPids(request);
		var localAttribute = isSpecimen ? NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID : PRIMARY_MEDIA_ID;
		return getPidName(responseJsonNode, localAttribute);
	}

	public void updatePid(List<JsonNode> request) throws PidException {
		pidClient.updatePids(request);
	}

	public void rollbackPidUpdate(List<JsonNode> request) throws PidException {
		log.info("Rolling back PID update");
		pidClient.rollbackPidsUpdate(request);
	}

	private HashMap<String, String> getPidName(JsonNode pidResponse, FdoProfileAttributes localAttribute)
			throws PidException {
		try {
			var dataNode = pidResponse.get("data");
			HashMap<String, String> pids = new HashMap<>();
			if (!dataNode.isArray()) {
				log.error(UNEXPECTED_LOG, "Data is not an array", pidResponse);
				throw new PidException(UNEXPECTED_MSG);
			}
			for (var node : dataNode) {
				var doi = node.get("id");
				var localId = node.get("attributes").get(localAttribute.getAttribute());
				pids.put(localId.asString(), doi.asString());
			}
			return pids;
		}
		catch (NullPointerException _) {
			log.error(UNEXPECTED_LOG, "Unexpected null", pidResponse);
			throw new PidException(UNEXPECTED_MSG);
		}
	}

}
