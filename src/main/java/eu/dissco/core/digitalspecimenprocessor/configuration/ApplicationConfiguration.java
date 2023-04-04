package eu.dissco.core.digitalspecimenprocessor.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Random;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationConfiguration {

  @Bean
  public ObjectMapper objectMapper() {
    var mapper = new ObjectMapper();
    mapper.findAndRegisterModules();
    return mapper;
  }

  @Bean
  public Random random() {
    return new Random();
  }

  @Bean
  public DocumentBuilder documentBuilder() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    docFactory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
    return docFactory.newDocumentBuilder();
  }

  @Bean
  public TransformerFactory transformerFactory() {
    var factory = TransformerFactory.newInstance();
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
    factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
    return factory;
  }

}
