package eu.dissco.core.digitalspecimenprocessor.service;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;

@Component
@RequiredArgsConstructor
@Slf4j
public class HandleService {

  private final WebClient webClient;

  public void postHandle(DigitalSpecimen specimen){
    //

  }



}
