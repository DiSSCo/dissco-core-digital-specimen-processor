package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
@Slf4j
public class TokenAuthenticator {

  private final TokenProperties properties;

  @Qualifier("tokenClient")
  private final WebClient tokenClient;

  public String getToken() throws PidAuthenticationException {
    var response = tokenClient
        .post()
        .body(BodyInserters.fromFormData(properties.getFromFormData()))
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve()
        .onStatus(HttpStatus.UNAUTHORIZED::equals,
            r -> Mono.error(new PidAuthenticationException("Service is unauthorized.")))
        .bodyToMono(JsonNode.class)
        .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(2))
            .filter(WebClientUtils::is5xxServerError)
            .onRetryExhaustedThrow((retryBackoffSpec, retrySignal) ->
                new PidAuthenticationException(
                    "External Service failed to process after max retries")
            ));
    try {
      var tokenNode = response.toFuture().get();
      return getToken(tokenNode);
    } catch (InterruptedException | ExecutionException e) {
      Thread.currentThread().interrupt();
      log.info("Unable to authenticate processing service with Keycloak. Verify client secret is up to-date");
      throw new PidAuthenticationException(
          "Unable to authenticate processing service with Keycloak. More information: "
              + e.getMessage());
    }
  }

  private String getToken(JsonNode tokenNode) throws PidAuthenticationException {
    if (tokenNode != null && tokenNode.get("access_token") != null) {
      return tokenNode.get("access_token").asText();
    }
    log.debug("Unexpected response from keycloak server. Unable to parse access_token");
    throw new PidAuthenticationException(
        "Unable to authenticate processing service with Keycloak. An error has occurred parsing keycloak response");
  }

}
