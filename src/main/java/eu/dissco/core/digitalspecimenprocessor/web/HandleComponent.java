package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_MEDIA_ID;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final String UNEXPECTED_LOG = "Unexpected response from Handle API. Error: {}. Response: {}";

  public Map<String, String> postHandle(List<JsonNode> request, boolean isSpecimen)
      throws PidException {
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.POST, requestBody, "batch");
    var responseJsonNode = getFutureResponse(response);
    var localAttribute = isSpecimen ? NORMALISED_PRIMARY_SPECIMEN_OBJECT_ID : PRIMARY_MEDIA_ID;
    return getHandleName(responseJsonNode, localAttribute);
  }

  public void updateHandle(List<JsonNode> request)
      throws PidException {
    log.info("Patching Digital Specimens to Handle API");
    var requestBody = BodyInserters.fromValue(request);
    var response = sendRequest(HttpMethod.PATCH, requestBody, "");
    getFutureResponse(response);
  }

  public void rollbackHandleCreation(Set<String> handles)
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
    } catch (ExecutionException e){
      log.error("Unable to rollback handles based on physical identifier: {}", physIds);
    } catch (InterruptedException e){
      Thread.currentThread().interrupt();
      log.error("A critical interrupted exception has occurred.");
      throw new PidException("Interrupted exception");
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

  private HashMap<String, String> getHandleName(JsonNode handleResponse, FdoProfileAttributes localAttribute)
      throws PidException {
    try {
      var dataNode = handleResponse.get("data");
      HashMap<String, String> handleNames = new HashMap<>();
      if (!dataNode.isArray()) {
        log.error(UNEXPECTED_LOG, "Data is not an array", handleResponse);
        throw new PidException(UNEXPECTED_MSG);
      }
      for (var node : dataNode) {
        var doi = node.get("id");
        var localId = node.get("attributes").get(localAttribute.getAttribute());
        handleNames.put(localId.asText(), doi.asText());
      }
      return handleNames;
    } catch (NullPointerException e) {
      log.error(UNEXPECTED_LOG, "Unexpected null", handleResponse);
      throw new PidException(UNEXPECTED_MSG);
    }
  }
}
