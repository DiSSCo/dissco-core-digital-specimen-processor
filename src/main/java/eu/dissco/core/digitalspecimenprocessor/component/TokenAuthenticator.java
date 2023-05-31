package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

@Component
@RequiredArgsConstructor
public class TokenAuthenticator {

    private final TokenProperties properties;

    @Qualifier("tokenClient")
    private final WebClient tokenClient;

    public String getToken() throws PidAuthenticationException {
        var responseSpec = tokenClient
                .post()
                .body(BodyInserters.fromFormData(properties.getFromFormData()))
                .acceptCharset(StandardCharsets.UTF_8)
                .retrieve()
                .onStatus(HttpStatus.UNAUTHORIZED::equals,
                        r -> r.bodyToMono(String.class).map(PidAuthenticationException::new));

        var response = responseSpec.bodyToMono(JsonNode.class);
        try {
            var tokenNode = response.toFuture().get();
            var token = tokenNode.get("access_token");
            if(token==null){
                throw new PidAuthenticationException("Unable to authenticate processing service with Keycloak. An error has occurred connecting with the keycloak server.");
            }
            return token.asText();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (ExecutionException e) {
            throw new PidAuthenticationException("Unable to authenticate processing service with Keycloak. More information: " + e.getMessage());
        }
    }

}
