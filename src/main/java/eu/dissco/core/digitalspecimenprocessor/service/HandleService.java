package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.component.TokenAuthenticator;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class HandleService {

    @Qualifier("handleClient")
    private final WebClient handleClient;

    private final TokenAuthenticator tokenAuthenticator;


    public JsonNode postHandle(JsonNode requestBody)throws PidAuthenticationException, PidCreationException {
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
