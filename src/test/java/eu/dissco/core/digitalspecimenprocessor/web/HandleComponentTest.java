package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SECOND_HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenHandleRequestMin;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenMediaPidResponse;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUpdateHandleRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import eu.dissco.core.digitalspecimenprocessor.utils.TestUtils;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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

@ExtendWith(MockitoExtension.class)
class HandleComponentTest {

  private static MockWebServer mockHandleServer;
  @Mock
  private TokenAuthenticator tokenAuthenticator;
  private HandleComponent handleComponent;

  @BeforeAll
  static void init() throws IOException {
    mockHandleServer = new MockWebServer();
    mockHandleServer.start();
  }

  @AfterAll
  static void destroy() throws IOException {
    mockHandleServer.shutdown();
  }

  @BeforeEach
  void setup() {
    WebClient webClient = WebClient.create(
        String.format("http://%s:%s", mockHandleServer.getHostName(), mockHandleServer.getPort()));
    handleComponent = new HandleComponent(webClient, tokenAuthenticator, MAPPER);
  }

  @Test
  void testPostHandle() throws Exception {
    // Given
    var requestBody = List.of(TestUtils.givenHandleRequest());
    var responseBody = givenHandleResponse();
    var expected = Map.of(PHYSICAL_SPECIMEN_ID, HANDLE);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody);

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testPostMediaHandle() throws Exception {
    // Given
    var expected = givenMediaPidResponse();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody("""
            {
              "data": [{
                "id":"20.5000.1025/ZZZ-ZZZ-ZZZ",
                "attributes": {
                  "digitalMediaKey": {
                    "digitalSpecimenId":"20.5000.1025/V1Z-176-LL4",
                    "mediaUrl":"https://an-image.org"
                  }
                }
              }]
            }
            """)
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postMediaHandle(List.of(MAPPER.createObjectNode()));

    // Then
    assertThat(response).isEqualTo(expected);
  }

  @Test
  void testPostMediaHandleBadResponse() {
    // Given
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody("""
            {
              "data": {
                "id":"20.5000.1025/ZZZ-ZZZ-ZZZ",
                "attributes": {
                  "digitalMediaKey": {
                    "digitalSpecimenId":"20.5000.1025/V1Z-176-LL4",
                    "mediaUrl":"https://an-image.org"
                  }
                }
              }
            }
            """)
        .addHeader("Content-Type", "application/json"));

    // When / Then
    assertThrows(PidException.class,
        () -> handleComponent.postMediaHandle(List.of(MAPPER.createObjectNode())));
  }

  @Test
  void testPostMediaHandleNPE() {
    // Given
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody("""
            {
              "data": {
                "attributes": {
                  "digitalMediaKey": {
                    "digitalSpecimenId":"20.5000.1025/V1Z-176-LL4",
                    "mediaUrl":"https://an-image.org"
                  }
                }
              }
            }
            """)
        .addHeader("Content-Type", "application/json"));

    // When / Then
    assertThrows(PidException.class,
        () -> handleComponent.postMediaHandle(List.of(MAPPER.createObjectNode())));
  }


  @Test
  void testUpdateHandle() throws Exception {
    // Given
    var requestBody = List.of(TestUtils.givenHandleRequest());
    var responseBody = TestUtils.givenHandleRequest();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When / Then
    assertDoesNotThrow(() -> handleComponent.updateHandle(requestBody));
  }

  @Test
  void testUnauthorized() throws Exception {
    // Given
    var requestBody = List.of(TestUtils.givenHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.UNAUTHORIZED.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  @Test
  void testBadRequest() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.BAD_REQUEST.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  @Test
  void testRetriesSuccess() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    var responseBody = givenHandleResponse();
    var expected = Map.of(PHYSICAL_SPECIMEN_ID, HANDLE);
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    // When
    var response = handleComponent.postHandle(requestBody
    );

    // Then
    assertThat(response).isEqualTo(expected);
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(2);
  }

  @Test
  void testRollbackFromPhysId() {
    //
    mockHandleServer.enqueue(new MockResponse().setResponseCode(200));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackFromPhysId(List.of(PHYSICAL_SPECIMEN_ID)));
  }

  @Test
  void testRollbackHandleCreation() {
    // Given
    var requestBody = List.of(HANDLE, SECOND_HANDLE);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleCreation(requestBody));
  }

  @Test
  void testRollbackHandleUpdate() throws Exception {
    // Given
    var requestBody = givenHandleRequestMin();
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .addHeader("Content-Type", "application/json"));

    // Then
    assertDoesNotThrow(() -> handleComponent.rollbackHandleUpdate(List.of(requestBody)));
  }

  @Test
  void testInterruptedException() throws Exception {
    // Given
    var requestBody = givenHandleRequestMin();
    var responseBody = TestUtils.givenHandleRequest();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));

    Thread.currentThread().interrupt();

    // When
    var response = assertThrows(PidException.class,
        () -> handleComponent.postHandle(List.of(requestBody)
        ));

    // Then
    assertThat(response).hasMessage(
        "Interrupted execution: A connection error has occurred in creating a handle.");
  }

  @Test
  void testRetriesFail() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    int requestCount = mockHandleServer.getRequestCount();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));
    mockHandleServer.enqueue(new MockResponse().setResponseCode(501));

    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
    assertThat(mockHandleServer.getRequestCount() - requestCount).isEqualTo(4);
  }

  @Test
  void testDataNodeNotArray() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    var responseBody = MAPPER.createObjectNode();
    responseBody.put("data", "val");
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  @Test
  void testDataMissingId() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    var responseBody = MAPPER.readTree("""
        {
          "data": [
            {
              "type": "digitalSpecimen",
              "attributes": {
                "fdoProfile": "https://doi.org/21.T11148/d8de0819e144e4096645",
                "digitalObjectType": "https://doi.org/21.T11148/894b1e6cad57e921764ee",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "primarySpecimenObjectId": "https://geocollections.info/specimen/23602",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "primarySpecimenObjectIdType": "cetaf",
                "referentName": "Biota",
                "topicDiscipline": "Earth Systems",
                "livingOrPreserved": "living",
                "markedAsType": true
              }
            }
          ]
        }
        """);

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  @Test
  void testDataMissingPhysicalId() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    var responseBody = MAPPER.readTree("""
        {
          "data": [
            {
              "id": "20.5000.1025/V1Z-176-LL4",
              "type": "digitalSpecimen",
              "attributes": {
                "fdoProfile": "https://doi.org/21.T11148/d8de0819e144e4096645",
                "digitalObjectType": "https://doi.org/21.T11148/894b1e6cad57e921764ee",
                "issuedForAgent": "https://ror.org/0566bfb96",
                "specimenHost": "https://ror.org/0443cwa12",
                "specimenHostName": "National Museum of Natural History",
                "referentName": "Biota",
                "topicDiscipline": "Earth Systems",
                "livingOrPreserved": "living",
                "markedAsType": true
              }
            }
          ]
        }
        """);
    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  @Test
  void testEmptyResponse() throws Exception {
    // Given
    var requestBody = List.of(
        TestUtils.givenHandleRequest());
    var responseBody = MAPPER.createObjectNode();

    mockHandleServer.enqueue(new MockResponse().setResponseCode(HttpStatus.OK.value())
        .setBody(MAPPER.writeValueAsString(responseBody))
        .addHeader("Content-Type", "application/json"));
    // Then
    assertThrows(PidException.class, () -> handleComponent.postHandle(requestBody
    ));
  }

  private JsonNode givenHandleResponse() throws JsonProcessingException {
    return MAPPER.readTree("""
        {
          "data": [
            {
              "id": "20.5000.1025/V1Z-176-LL4",
              "attributes": {
                "normalisedPrimarySpecimenObjectId" : "https://geocollections.info/specimen/23602"
              }
            }
          ]
        }
        """);
  }

}
