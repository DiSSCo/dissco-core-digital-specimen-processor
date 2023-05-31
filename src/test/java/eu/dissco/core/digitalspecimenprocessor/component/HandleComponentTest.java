package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriBuilder;
import reactor.core.publisher.Mono;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.BDDMockito.given;
import static reactor.core.publisher.Mono.when;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {
    @Mock
    private WebClient webClient;
    @Mock
    private WebClient.RequestBodyUriSpec bodySpec;
    @Mock
    private WebClient.RequestHeadersSpec headerSpec;
    @Mock
    private WebClient.ResponseSpec responseSpec;
    @Mock
    private Mono<JsonNode> jsonNodeMono;
    @Mock
    private CompletableFuture<JsonNode> jsonFuture;
    @Mock
    TokenAuthenticator tokenAuthenticator;
    @Mock
    UriBuilder uriBuilder;
    private HandleComponent handleComponent;
    private static final String TOKEN = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

    @BeforeEach
    void setup() throws Exception {
        handleComponent = new HandleComponent(webClient, tokenAuthenticator);
        givenWebclient();
    }

    @Test
    void testPostHandle() throws Exception {
        // Given
        var requestBody = List.of(MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
        var expected = requestBody.get(0);
        given(jsonFuture.get()).willReturn(expected);

        // When
        var response = handleComponent.postHandle(requestBody);

        // Then
        Assertions.assertThat(response).isEqualTo(expected);
    }

    @Test
    void testPostHandleInterrupted() throws Exception {
        // Given
        given(jsonFuture.get()).willThrow(new InterruptedException());
        var requestBody = List.of(MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));

        // Then
        assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
    }



    private void givenWebclient() throws Exception {
        given(tokenAuthenticator.getToken()).willReturn(TOKEN);
        given(webClient.post()).willReturn(bodySpec);
        given(bodySpec.uri(any(Function.class))).willReturn(bodySpec);
        given(bodySpec.body(any())).willReturn(headerSpec);
        given(headerSpec.header("Authorization", ("Bearer " + TOKEN))).willReturn(headerSpec);
        given(headerSpec.acceptCharset(StandardCharsets.UTF_8)).willReturn(headerSpec);
        given(headerSpec.retrieve()).willReturn(responseSpec);
        given(responseSpec.bodyToMono(any(Class.class))).willReturn(jsonNodeMono);
        given(jsonNodeMono.toFuture()).willReturn(jsonFuture);
    }

}
