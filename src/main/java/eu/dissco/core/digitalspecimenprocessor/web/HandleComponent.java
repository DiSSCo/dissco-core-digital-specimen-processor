package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.DIGITAL_MEDIA_KEY;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaKey;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
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
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

  @Qualifier("handleClient")
  private final WebClient handleClient;
  private final TokenAuthenticator tokenAuthenticator;
  private final ObjectMapper mapper;

  private static final String UNEXPECTED_MSG = "Unexpected response from handle API";
  private static final String UNEXPECTED_LOG = "Unexpected response from Handle API. Missing id and/or primarySpecimenObjectId. Response: {}";

  public Map<String, String> postHandle(List<JsonNode> request)
      throws PidException {
    log.info("Posting Digital Specimens to Handle API");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    var responseJsonNode = getFutureResponse(response);
    return getHandleName(responseJsonNode);
  }

  public Map<DigitalMediaKey, String> postMediaHandle(List<JsonNode> request)
      throws PidException {
    log.info("Posting Digital Specimens to Handle API");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    var responseJsonNode = getFutureResponse(response);
    return getHandleNameMedia(responseJsonNode);
  }

  public void updateHandle(List<JsonNode> request)
      throws PidException {
    log.info("Patching Digital Specimens to Handle API");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PATCH, requestBody, "");
    getFutureResponse(response);
  }

  public void rollbackHandleCreation(List<String> handles)
      throws PidException {
    log.info("Rolling back handle creation");
    var requestBody = BodyInserters.fromValue(handles);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/create");
    getFutureResponse(response);
  }

  public void rollbackFromPhysId(List<String> physIds) throws PidException{
    log.info("Rolling back handles from phys ids");
    try {
      var requestBody = BodyInserters.fromValue(physIds);
      var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/physId");
      response.toFuture().get();
    } catch (InterruptedException | ExecutionException e){
      Thread.currentThread().interrupt();
      log.error("Unable to rollback handles based on physical identifier: {}", physIds);
    }
  }

  public void rollbackHandleUpdate(List<JsonNode> request)
      throws PidException {
    log.info("Rolling back handle update");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.DELETE, requestBody, "rollback/update");
    getFutureResponse(response);
  }

  private <T> Mono<JsonNode> sendRequest(HttpMethod httpMethod,
      BodyInserter<T, ReactiveHttpOutputMessage> requestBody, String endpoint) throws PidException {
    var token = "Bearer " + tokenAuthenticator.getToken();
    return handleClient.method(httpMethod)
        .uri(uriBuilder -> uriBuilder.path(endpoint).build())
        .body(requestBody).header("Authorization", token)
        .acceptCharset(StandardCharsets.UTF_8).retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals, r -> Mono.error(
            new PidException("Unable to authenticate with Handle Service.")))
        .onStatus(HttpStatusCode::is4xxClientError, r -> Mono.error(new PidException(
            "Unable to create PID. Response from Handle API: " + r.statusCode())))
        .bodyToMono(JsonNode.class).retryWhen(
            Retry.fixedDelay(3, Duration.ofSeconds(2)).filter(WebClientUtils::is5xxServerError)
                .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) -> new PidException(
                    "External Service failed to process after max retries")));
  }

  private JsonNode getFutureResponse(Mono<JsonNode> response)
      throws PidException {
    try {
      return response.toFuture().get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.error("Interrupted exception has occurred.");
      throw new PidException(
          "Interrupted execution: A connection error has occurred in creating a handle.");
    } catch (ExecutionException e) {
      log.error("An unexpected exception has occurred while reading handle API response", e);
      throw new PidException(e.getCause().getMessage());
    }
  }

  private HashMap<String, String> getHandleName(JsonNode handleResponse)
      throws PidException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<String, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
        throw new PidException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var handle = node.get("id");
        var localId = node.get("attributes")
            .get(NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID.getAttribute());
        if (handle == null || localId == null) {
          log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
          throw new PidException(UNEXPECTED_MSG);
        }
        handleNames.put(localId.asText(), handle.asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
      throw new PidException(UNEXPECTED_MSG);
    }
  }

  private HashMap<DigitalMediaKey, String> getHandleNameMedia(JsonNode handleResponse)
      throws PidException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<DigitalMediaKey, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
        throw new PidException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var handle = node.get("id");
        var localId = node.get("attributes").get(DIGITAL_MEDIA_KEY.getAttribute());
        if (handle == null || localId == null) {
          log.error(UNEXPECTED_LOG, handleResponse.toPrettyString());
          throw new PidException(UNEXPECTED_MSG);
        }
        handleNames.put(mapper.treeToValue(localId, DigitalMediaKey.class), handle.asText());
      }
      return handleNames;
    } catch (NullPointerException | JsonProcessingException e) {
      log.error(UNEXPECTED_LOG, handleResponse.toPrettyString(), e);
      throw new PidException(UNEXPECTED_MSG);
    }
  }
}
