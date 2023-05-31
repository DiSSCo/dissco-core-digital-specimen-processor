package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleComponent {

    @Qualifier("handleClient")
    private final WebClient handleClient;

    private final TokenAuthenticator tokenAuthenticator;

    public JsonNode postHandle(List<JsonNode> requestBody)throws PidAuthenticationException, PidCreationException {
        var token = "Bearer " + tokenAuthenticator.getToken();
        var response = handleClient.post()
                .uri(uriBuilder -> uriBuilder.path("batch").build())
                .body(BodyInserters.fromValue(requestBody))
                .header("Authorization", token)
                .acceptCharset(StandardCharsets.UTF_8)
                .retrieve()
                .bodyToMono(JsonNode.class);

        try {
            return response.toFuture().get();
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            log.info(response.toString());
            throw new PidCreationException("An error has occurred in creating a PID. More information: " + e.getMessage());
        }
    }
}
