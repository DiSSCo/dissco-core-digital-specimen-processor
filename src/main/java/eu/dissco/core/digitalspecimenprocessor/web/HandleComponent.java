package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ReactiveHttpOutputMessage;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

  @Qualifier("handleClient")
  private final WebClient handleClient;
  private final TokenAuthenticator tokenAuthenticator;

  private static final String UNEXPECTED_MSG = "Unexpected response from handle API";
  private static final String UNEXPECTED_LOG = "Unexpected response from Handle API. Missing id and/or primarySpecimenObjectId. Response: {}";

  public Map<String, String> postHandle(List<JsonNode> request)
      throws PidAuthenticationException, PidCreationException {
    log.info("Posting Digital Specimens to Handle API");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PATCH, requestBody, "upsert");
    var responseJsonNode = getFutureResponse(response);
    return getHandleName(responseJsonNode);
  }

  public void rollbackHandleCreation(JsonNode request)
      throws PidCreationException, PidAuthenticationException {
    log.info("Rolling back handle creation");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback");
    getFutureResponse(response);
  }

  public void rollbackHandleUpdate(List<JsonNode> request)
      throws PidCreationException, PidAuthenticationException {
    log.info("Rolling back handle update");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/update");
    getFutureResponse(response);
  }

  public void registerDois(List<String> handles)
      throws PidAuthenticationException, PidCreationException {
    var requestBody = BodyInserters.fromValue(handles);
    var response = sendRequest(HttpMethod.POST, requestBody, "dois");
    getFutureResponse(response);
  }

  private <T> Mono<JsonNode> sendRequest(HttpMethod httpMethod,
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint)
      throws PidAuthenticationException {
    var token = "Bearer " + tokenAuthenticator.getToken();
    return handleClient.method(httpMethod)
        .uri(uriBuilder -> uriBuilder.path(endpoint).build())
        .body(requestBody).header("Authorization", token)
        .acceptCharset(StandardCharsets.UTF_8).retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals, r -> Mono.error(
            new PidAuthenticationException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError, r -> Mono.error(new PidCreationException(
            "Unable to create PID. Response from Handle API: " + r.statusCode())))
        .bodyToMono(JsonNode.class).retryWhen(
            Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(WebClientUtils::is5xxServerError)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new PidCreationException(
                    "External Service failed to process after max retries")));
  }

  private JsonNode getFutureResponse(Mono<JsonNode> response)
      throws PidCreationException, PidAuthenticationException {
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted exception has occurred.");
      throw new PidCreationException(
          "Interrupted execution: A connection error has occurred in creating a handle.");
    } catch (ExecutionException e) {
      if (e.getCause().getClass().equals(PidAuthenticationException.class)) {
        log.error(
            "Token obtained from Keycloak not accepted by Handle Server. Check Keycloak configuration.");
        throw new PidAuthenticationException(e.getCause().getMessage());
      }
      log.error("An unexpected exception has occurred while reading handle API response", e);
      throw new PidCreationException(e.getCause().getMessage());
    }
  }

  private HashMap<String, String> getHandleName(JsonNode handleResponse)
      throws PidCreationException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<String, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
        throw new PidCreationException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var handle = node.get("id");
        var primarySpecimenObjectId = node.get("attributes")
            .get(PRIMARY_SPECIMEN_OBJECT_ID.getAttribute());
        if (handle == null || primarySpecimenObjectId == null) {
          log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
          throw new PidCreationException(UNEXPECTED_MSG);
        }
        handleNames.put(primarySpecimenObjectId.asText(), handle.asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
      throw new PidCreationException(UNEXPECTED_MSG);
    }
  }
}
