package eu.dissco.core.digitalspecimenprocessor.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {

  @Bean
  public ObjectMapper objectMapper() {
    return new ObjectMapper().findAndRegisterModules();
  }

  @Bean
  public Random random() {
    return new Random();
  }

  @Bean
  public DocumentBuilder documentBuilder() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    return docFactory.newDocumentBuilder();
  }

}
