package eu.dissco.core.digitalspecimenprocessor.configuration;

import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.reactive.function.client.ClientResponse;
import reactor.core.publisher.Mono;
import tools.jackson.databind.JsonNode;

@Slf4j
public class WebClientErrorHandling {

  private WebClientErrorHandling(){}

  public static Mono<ClientResponse> exchangeFilterResponseProcessor(ClientResponse response) {
    var status = response.statusCode();
    if (status.is4xxClientError() || status.is5xxServerError()) {
      return response.bodyToMono(JsonNode.class)
          .flatMap(body -> {
            log.error("An error has occurred with the DOI service. Status: {}, response: {}", status, body);
            return Mono.error(
                new PidException(
                    "An error has occurred with the DOI service"));
          });
    }
    return Mono.just(response);
  }


}
