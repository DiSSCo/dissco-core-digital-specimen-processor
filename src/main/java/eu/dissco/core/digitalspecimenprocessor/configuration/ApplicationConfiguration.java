package eu.dissco.core.digitalspecimenprocessor.configuration;

import com.fasterxml.jackson.annotation.JsonSetter.Value;
import com.fasterxml.jackson.annotation.Nulls;
import java.text.SimpleDateFormat;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import tools.jackson.databind.json.JsonMapper;

@Configuration
public class ApplicationConfiguration {

	public static final String DATE_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

	@Bean
	public JsonMapper jsonMapper() {
		return JsonMapper.builder()
			.findAndAddModules()
			.defaultDateFormat(new SimpleDateFormat(DATE_STRING))
			.defaultTimeZone(TimeZone.getTimeZone(ZoneOffset.UTC))
			.withConfigOverride(List.class, cfg -> cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
			.withConfigOverride(Map.class, cfg -> cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
			.withConfigOverride(Set.class, cfg -> cfg.setNullHandling(Value.forValueNulls(Nulls.AS_EMPTY)))
			.build();
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
