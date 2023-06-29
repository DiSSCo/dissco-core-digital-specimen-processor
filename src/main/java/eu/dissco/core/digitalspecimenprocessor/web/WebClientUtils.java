package eu.dissco.core.digitalspecimenprocessor.web;

import org.springframework.web.reactive.function.client.WebClientResponseException;

public class WebClientUtils {

  public static boolean is5xxServerError(Throwable throwable) {
    return throwable instanceof WebClientResponseException webClientResponseException
        && webClientResponseException.getStatusCode().is5xxServerError();
  }

  private WebClientUtils(){}

}
