package eu.dissco.core.digitalspecimenprocessor.component;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.loadResourceFile;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.exception.PidAuthenticationException;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.service.KafkaPublisherService;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.WebClient;

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {
  @Mock
  TokenAuthenticator tokenAuthenticator;
  @Mock
  KafkaPublisherService kafkaService;
  private HandleComponent handleComponent;

  private static MockWebServer mockHandleServer;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @BeforeEach
  void setup()  {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator, kafkaService);

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
    var expected = requestBody.get(0);
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(expected))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody, new ArrayList<>());

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testUnauthorized() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var digitalSpecimen = givenDigitalSpecimen();
    var expectedEvent = List.of(new DigitalSpecimenEvent(new ArrayList<>(), digitalSpecimen));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidAuthenticationException.class, () -> handleComponent.postHandle(requestBody, List.of(digitalSpecimen)));
    then(kafkaService).should().deadLetterEvent(expectedEvent);
  }

  @Test
  void testBadRequest() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var digitalSpecimen = givenDigitalSpecimen();
    var expectedEvent = List.of(new DigitalSpecimenEvent(new ArrayList<>(), digitalSpecimen));

    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody, List.of(digitalSpecimen)));
    then(kafkaService).should().deadLetterEvent(expectedEvent);
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var digitalSpecimen = givenDigitalSpecimen();
    var expected = requestBody.get(0);
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse()
        .setResponseCode(HttpStatus.CREATED.value())
        .setBody(MAPPER.writeValueAsString(expected))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody, List.of(digitalSpecimen));

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockHandleServer.getRequestCount()-requestCount).isEqualTo(2);
  }

  @Test
  void testRetriesFail() throws Exception {
    // Given
    var requestBody = List.of(
        MAPPER.readTree(loadResourceFile("handlerequests/TestHandleRequestFullTypeStatus.json")));
    var digitalSpecimen = givenDigitalSpecimen();
    var expectedEvent = List.of(new DigitalSpecimenEvent(new ArrayList<>(), digitalSpecimen));
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidCreationException.class, () -> handleComponent.postHandle(requestBody, List.of(digitalSpecimen)));
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(4);
    then(kafkaService).should().deadLetterEvent(expectedEvent);
  }

}
