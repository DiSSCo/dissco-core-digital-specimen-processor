package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.given;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.property.TokenProperties;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class TokenAuthenticatorTest {


  private static MockWebServer mockTokenServer;

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

  @BeforeAll
  static void init() throws IOException {
    mockTokenServer = new MockWebServer();
    mockTokenServer.start();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockTokenServer.getHostName(), mockTokenServer.getPort()));
    authenticator = new TokenAuthenticator(properties, webClient);
  }

  @AfterAll
  static void destroy() throws IOException {
    mockTokenServer.shutdown();
  }

  @Test
  void testGetToken() throws Exception {
    // Given
    var expectedJson = givenTokenResponse();
    var expected = expectedJson.get("access_token").asText();
    given(properties.getFromFormData()).willReturn(testFromFormData);

    mockTokenServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(expectedJson))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = authenticator.getToken();

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testGetTokenUnauthorized() {
    // Given
    given(properties.getFromFormData()).willReturn(testFromFormData);
    mockTokenServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidAuthenticationException.class, () -> authenticator.getToken());
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    int requestCount = mockTokenServer.getRequestCount();
    var expectedJson = givenTokenResponse();
    var expected = expectedJson.get("access_token").asText();
    given(properties.getFromFormData()).willReturn(testFromFormData);
    mockTokenServer.enqueue(new MockResponse().setResponseCode(501));
    mockTokenServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(expectedJson))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = authenticator.getToken();

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockTokenServer.getRequestCount() - requestCount).isEqualTo(2);
  }

  @Test
  void testRetriesFailure() throws Exception {
    // Given
    int requestCount = mockTokenServer.getRequestCount();
    var expectedJson = givenTokenResponse();
    var expected = expectedJson.get("access_token").asText();
    given(properties.getFromFormData()).willReturn(testFromFormData);
    mockTokenServer.enqueue(new MockResponse().setResponseCode(501));
    mockTokenServer.enqueue(new MockResponse().setResponseCode(501));
    mockTokenServer.enqueue(new MockResponse().setResponseCode(501));
    mockTokenServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidAuthenticationException.class, () -> authenticator.getToken());
    assertThat(mockTokenServer.getRequestCount() - requestCount).isEqualTo(4);
  }

  @Test
  void testGetResponseIsNull() {
    given(properties.getFromFormData()).willReturn(testFromFormData);
    mockTokenServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // When
    assertThrows(PidAuthenticationException.class, () -> authenticator.getToken());
  }

  @Test
  void testGetTokenIsNull() {
    given(properties.getFromFormData()).willReturn(testFromFormData);
    mockTokenServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json")
        .setBody("{}"));

    // When
    assertThrows(PidAuthenticationException.class, () -> authenticator.getToken());
  }

  private static JsonNode givenTokenResponse() throws Exception {
    return MAPPER.readTree("""
        {
          "access_token": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
          aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
          aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\
          aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
          "expires_in": 3600,
          "refresh_expires_in": 0,
          "token_type": "Bearer",
          "not-before-policy": 0,
          "scope": ""
        }
        """);
  }

}
