package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.web.reactive.function.client.WebClient;

@Slf4j
class HandleServiceTest {

  @Mock
  WebClient webClient;
  private DigitalSpecimen specimen = givenDigitalSpecimen();
  private HandleService service = new HandleService(webClient);


}
