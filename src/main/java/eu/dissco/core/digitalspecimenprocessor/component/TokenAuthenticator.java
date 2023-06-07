package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import java.time.Duration;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

@Component
@RequiredArgsConstructor
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
            r -> Mono.error(new PidAuthenticationException("Unable to create PID")))
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
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (ExecutionException e) {
      throw new PidAuthenticationException(
          "Unable to authenticate processing service with Keycloak. More information: "
              + e.getMessage());
    }
  }

  private String getToken(JsonNode tokenNode) throws PidAuthenticationException {
    if (tokenNode == null) {
      throw new PidAuthenticationException(
          "Unable to authenticate processing service with Keycloak. An error has occurred parsing response");
    }
    var token = tokenNode.get("access_token");
    if (token == null) {
      throw new PidAuthenticationException(
          "Unable to authenticate processing service with Keycloak. An error has occurred connecting with the keycloak server.");
    }
    return token.asText();
  }

}
