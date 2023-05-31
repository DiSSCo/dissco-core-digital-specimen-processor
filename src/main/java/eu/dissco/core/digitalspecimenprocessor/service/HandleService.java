package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.component.TokenAuthenticator;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

    private final FdoRecordBuilder recordBuilder;

    public JsonNode postHandle(DigitalSpecimen specimen) {
        var token = "Bearer " + tokenAuthenticator.getToken();
        var requestBody = recordBuilder.genRequest(List.of(specimen));

        var response = handleClient.post().uri("api/v1/pids/batch")
                .body(BodyInserters.fromValue(requestBody))
                .header("Authorization", token)
                .acceptCharset(StandardCharsets.UTF_8)
                .retrieve()
                .bodyToMono(JsonNode.class);

        try {
            var r = response.toFuture().get();
            log.info(r.toPrettyString());
            return  r;
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }


}
