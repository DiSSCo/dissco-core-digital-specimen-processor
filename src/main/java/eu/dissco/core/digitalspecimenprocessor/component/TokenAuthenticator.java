package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Qualifier;
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

    public JsonNode getToken() throws ExecutionException {
        var responseSpec = tokenClient
                .post()
                .body(BodyInserters.fromFormData(properties.getFromFormData()))
                .acceptCharset(StandardCharsets.UTF_8)
                .retrieve();

        var response = responseSpec.bodyToMono(JsonNode.class);
        try {
            return response.toFuture().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }

}
