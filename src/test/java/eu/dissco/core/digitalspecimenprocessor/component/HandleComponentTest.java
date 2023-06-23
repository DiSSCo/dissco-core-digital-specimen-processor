package eu.dissco.core.digitalspecimenprocessor.component;

import static eu.dissco.core.digitalspecimenprocessor.domain.FdoProfileAttributes.PRIMARY_SPECIMEN_OBJECT_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.BDDAssumptions.given;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
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
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  @Mock
  private TokenAuthenticator tokenAuthenticator;
  private HandleComponent handleComponent;

  private static MockWebServer mockHandleServer;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator);

  }

  @AfterAll
  static void destroy() throws IOException {
    mockHandleServer.shutdown();
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.readTree(loadResourceFile("handlerequests/TestHandleResponse.json"));
    var expected = givenHandleNameResponse(responseBody);
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testUnauthorized() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidAuthenticationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testBadRequest() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.readTree(loadResourceFile("handlerequests/TestHandleResponse.json"));
    var expected = givenHandleNameResponse(responseBody);
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(2);
  }


  @Test
  void testRollbackHandleCreation() throws Exception {
    // Given
    var requestBody = MAPPER.readTree(
        loadResourceFile("handlerequests/HandleRollbackCreationRequest.json"));
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(requestBody));
  }

  @Test
  void testRollbackHandleUpdate() throws Exception {
    // Given
    var requestBody = MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestMin.json"));
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(requestBody));
  }

  @Test
  void testInterruptedException() throws Exception {
    // Given
    var requestBody = MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestMin.json"));
    var responseBody = MAPPER.readTree(loadResourceFile("handlerequests/TestHandleResponse.json"));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    Thread.currentThread().interrupt();

    // When
    var response = assertThrows(PidCreationException.class, () -> handleComponent.postHandle(List.of(requestBody)));

    // Then
    assertThat(response).hasMessage("Interrupted execution: A connection error has occurred in creating a handle.");
  }

  @Test
  void testRetriesFail() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(4);
  }

  @Test
  void testDataNodeNotArray() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.createObjectNode();
    responseBody.put("data", "val");
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testDataMissingId() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.readTree(
        loadResourceFile("handlerequests/TestHandleResponseMissingId.json"));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testDataMissingPhysicalId() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.readTree(
        loadResourceFile("handlerequests/TestHandleResponseMissingPhysicalId.json"));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  @Test
  void testEmptyResponse() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var responseBody = MAPPER.createObjectNode();

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody));
  }

  private HashMap<String, String> givenHandleNameResponse(JsonNode responseBody) {
    HashMap<String, String> handleNames = new HashMap<>();
    for (var node : responseBody.get("data")) {
      handleNames.put(
          node.get("attributes").get(PRIMARY_SPECIMEN_OBJECT_ID.getAttribute()).asText(),
          node.get("id").asText());
    }
    return handleNames;
  }

}
