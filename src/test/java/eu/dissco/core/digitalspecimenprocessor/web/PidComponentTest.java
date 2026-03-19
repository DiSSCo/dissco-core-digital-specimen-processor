package eu.dissco.core.digitalspecimenprocessor.web;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_PID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MEDIA_URL;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenPidRequest;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;

import eu.dissco.core.digitalspecimenprocessor.client.PidClient;
import eu.dissco.core.digitalspecimenprocessor.exception.PidException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;

@ExtendWith(MockitoExtension.class)
class PidComponentTest {

  private PidComponent pidComponent;
  @Mock
  private PidClient pidClient;


  @BeforeEach
  void setup() {
    pidComponent = new PidComponent(pidClient);
  }

  @Test
  void testPostSpecimenPid() throws Exception {
    // Given
    given(pidClient.postPids(any())).willReturn(givenPidResponse());
    var expected = Map.of(PHYSICAL_SPECIMEN_ID, HANDLE);

    // When
    var result = pidComponent.postPid(List.of(givenPidRequest()), true);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testPostMediaPid() throws Exception {
    // Given
    given(pidClient.postPids(any())).willReturn(givenPidResponseMedia());
    var expected = Map.of(MEDIA_URL, MEDIA_PID);

    // When
    var result = pidComponent.postPid(List.of(givenPidRequest()), false);

    // Then
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testPostPidBadResponse() throws Exception {
    // given
    var response = MAPPER.readTree("""
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
        """);
    given(pidClient.postPids(any())).willReturn(response);

    // When / Then
    assertThrows(PidException.class,
        () -> pidComponent.postPid(List.of(givenPidRequest()), true));
  }

  @Test
  void testPostPidNPE() throws Exception {
    // given
    var response = MAPPER.readTree("""
        {
          "data": [{
          }]
        }
        """);
    given(pidClient.postPids(any())).willReturn(response);

    // When / Then
    assertThrows(PidException.class,
        () -> pidComponent.postPid(List.of(givenPidRequest()), true));
  }
  
  @Test
  void testUpdatePid() throws Exception {
    // Given
    
    // When 
    pidComponent.updatePid(List.of(givenPidRequest()));
    
    // Then
    then(pidClient).should().updatePids(List.of(givenPidRequest()));
  }

  @Test
  void testRollbackPid() throws Exception {
    // Given

    // When 
    pidComponent.rollbackPidUpdate(List.of(givenPidRequest()));

    // Then
    then(pidClient).should().rollbackPidsUpdate(List.of(givenPidRequest()));
  }

  private JsonNode givenPidResponse() {
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

  private JsonNode givenPidResponseMedia() {
    return MAPPER.readTree("""
        {
                      "data": [{
                        "id":"20.5000.1025/ZZZ-ZZZ-ZZZ",
                        "attributes": {
                            "primaryMediaId":"https://an-image.org"
                        }
                      }]
                    }
        """);

  }

}
