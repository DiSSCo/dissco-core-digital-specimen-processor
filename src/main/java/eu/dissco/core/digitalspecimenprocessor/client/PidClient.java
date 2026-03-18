package eu.dissco.core.digitalspecimenprocessor.client;

import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.util.List;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.service.annotation.DeleteExchange;
import org.springframework.web.service.annotation.PatchExchange;
import org.springframework.web.service.annotation.PostExchange;
import tools.jackson.databind.JsonNode;

public interface PidClient {

  @PostExchange("/batch")
  JsonNode postPids(@RequestBody List<JsonNode> requestBody) throws PidException;

  @PatchExchange("/")
  void updatePids(@RequestBody List<JsonNode> requestBody) throws PidException;

  @DeleteExchange("/rollback/update")
  void rollbackPidsUpdate(@RequestBody List<JsonNode> requestBody) throws PidException;

}
