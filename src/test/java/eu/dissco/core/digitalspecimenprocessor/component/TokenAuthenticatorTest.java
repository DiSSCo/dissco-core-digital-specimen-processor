package eu.dissco.core.digitalspecimenprocessor.component;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
class TokenAuthenticatorTest {
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
    private TokenProperties properties;
    private final MultiValueMap<String, String> testFromFormData = new LinkedMultiValueMap<>() {{
        add("grant_type", "grantType");
        add("client_id", "clientId");
        add("client_secret", "secret");
    }};

    @Mock
    private CompletableFuture<JsonNode> jsonFuture;
    private TokenAuthenticator authenticator;

    @BeforeEach
    void setup() {
        authenticator = new TokenAuthenticator(properties, webClient);
        givenWebclient();
    }

    @Test
    void testGetToken() throws Exception {
        // Given
        var expectedJson = MAPPER.readTree(loadResourceFile("webclientresponse/tokenResponse.json"));
        given(jsonFuture.get()).willReturn(expectedJson);
        var expected = expectedJson.get("access_token").asText();

        // When
        var response = authenticator.getToken();

        // Then
        assertThat(response).isEqualTo(expected);
    }

    @Test
    void testGetTokenInterrupted() throws Exception {
        // Given
        given(jsonFuture.get()).willThrow(new InterruptedException());

        // When
        var response = authenticator.getToken();

        // Then
        assertThat(response).isNull();
    }

    private void givenWebclient() {
        given(properties.getFromFormData()).willReturn(testFromFormData);
        given(webClient.post()).willReturn(bodySpec);
        given(bodySpec.body(any())).willReturn(headerSpec);
        given(headerSpec.acceptCharset(StandardCharsets.UTF_8)).willReturn(headerSpec);
        given(headerSpec.retrieve()).willReturn(responseSpec);
        given(responseSpec.bodyToMono(any(Class.class))).willReturn(jsonNodeMono);
        given(jsonNodeMono.toFuture()).willReturn(jsonFuture);
    }


}
