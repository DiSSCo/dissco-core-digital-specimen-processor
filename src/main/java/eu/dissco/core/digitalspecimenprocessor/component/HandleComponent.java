package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.service.KafkaPublisherService;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

  @Qualifier("handleClient")
  private final WebClient handleClient;
  private final TokenAuthenticator tokenAuthenticator;
  private final KafkaPublisherService kafkaService;

  public JsonNode postHandle(List<JsonNode> requestBody, List<DigitalSpecimenEvent> digitalSpecimenEvents)
      throws PidAuthenticationException, PidCreationException, JsonProcessingException {
    var token = "Bearer " + tokenAuthenticator.getToken();
    var response = handleClient.post()
        .uri(uriBuilder -> uriBuilder.path("batch").build())
        .body(BodyInserters.fromValue(requestBody))
        .header("Authorization", token)
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals, r -> Mono.error(
            new PidAuthenticationException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError,
            r -> Mono.error(new PidCreationException("Unable to create PID")))
        .bodyToMono(JsonNode.class)
        .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2))
            .filter(WebClientUtils::is5xxServerError)
            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                new PidCreationException(
                    "External Service failed to process after max retries")
            ));
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } catch (ExecutionException e) {
      if (e.getCause().getClass().equals(PidAuthenticationException.class)) {
        kafkaService.deadLetterEvent(digitalSpecimenEvents);
        throw new PidAuthenticationException(e.getCause().getMessage());
      }
      if (e.getCause().getClass().equals(PidCreationException.class)) {
        kafkaService.deadLetterEvent(digitalSpecimenEvents);
        throw new PidCreationException(e.getCause().getMessage());
      }
    }
    kafkaService.deadLetterEvent(digitalSpecimenEvents);
    throw new PidCreationException("An unknown error has occurred in creating a handle.");
  }

}
