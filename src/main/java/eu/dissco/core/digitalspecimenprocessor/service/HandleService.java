package eu.dissco.core.digitalspecimenprocessor.service;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClient.ResponseSpec;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
@Slf4j
public class HandleService {

  private final WebClient webClient;
  private final FdoRecordBuilder recordBuilder;

  public void postHandle(DigitalSpecimen specimen) {
    var requestBody = recordBuilder.genRequest(List.of(specimen));

    var uriSpec = webClient.post();
    var bodySpec = uriSpec.uri("/api/v1/pids/");
    var headersSpec = bodySpec.body(Mono.just(requestBody), JsonNode.class);
    var responseSpec = headersSpec.header(
            HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
        .accept(MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML)
        .acceptCharset(StandardCharsets.UTF_8)
        .retrieve();

    var responseVal = headersSpec.exchangeToMono(response -> {
      if (response.statusCode().equals(HttpStatus.OK)) {
        return response.bodyToMono(String.class);
      } else if (response.statusCode().is4xxClientError()) {
        return Mono.just("Error response");
      } else {
        return response.createException()
            .flatMap(Mono::error);
      }
    });


  }

  public void getHandle(String handle) {

  }


}
