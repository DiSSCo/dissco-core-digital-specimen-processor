package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAS;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenAutoAcceptedAnnotation;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalMediaEventWithRelationship;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenJsonPatch;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenNewAcceptedAnnotation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.then;

import com.fasterxml.jackson.core.JsonProcessingException;
import eu.dissco.core.digitalspecimenprocessor.domain.AutoAcceptedAnnotation;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenEvent;
import eu.dissco.core.digitalspecimenprocessor.domain.specimen.DigitalSpecimenRecord;
import eu.dissco.core.digitalspecimenprocessor.property.RabbitMQProperties;
import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
@ExtendWith(MockitoExtension.class)
class RabbitMQServiceTest {

  private static RabbitMQContainer container;
  private static RabbitTemplate rabbitTemplate;
  private RabbitMQService rabbitMQService;
  @Mock
  private ProcessingService processingService;
  @Mock
  private ProvenanceService provenanceService;

  @BeforeAll
  static void setupContainer() throws IOException, InterruptedException {
    container = new RabbitMQContainer("rabbitmq:4.0.8-management-alpine");
    container.start();
    // Declare digital specimen exchange, queue and binding
    declareRabbitResources("digital-specimen-exchange", "digital-specimen-queue",
        "digital-specimen");
    // Declare dlq exchange, queue and binding
    declareRabbitResources("digital-specimen-exchange-dlq", "digital-specimen-queue-dlq",
        "digital-specimen-dlq");
    // Declare digital media exchange, queue and binding
    declareRabbitResources("digital-media-exchange", "digital-media-queue", "digital-media");
    // Declare auto annotation exchange, queue and binding
    declareRabbitResources("auto-annotation-exchange", "auto-annotation-queue", "auto-annotation");
    // Declare create update tombstone exchange, queue and binding
    declareRabbitResources("create-update-tombstone-exchange", "create-update-tombstone-queue",
        "create-update-tombstone");
    // Declare mas ocr exchange, queue and binding
    declareRabbitResources("mas-exchange", "mas-ocr-queue", "OCR");

    CachingConnectionFactory factory = new CachingConnectionFactory(container.getHost());
    factory.setPort(container.getAmqpPort());
    factory.setUsername(container.getAdminUsername());
    factory.setPassword(container.getAdminPassword());
    rabbitTemplate = new RabbitTemplate(factory);
    rabbitTemplate.setReceiveTimeout(100L);
  }


  private static void declareRabbitResources(String exchangeName, String queueName,
      String routingKey)
      throws IOException, InterruptedException {
    container.execInContainer("rabbitmqadmin", "declare", "exchange", "name=" + exchangeName,
        "type=direct", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "queue", "name=" + queueName,
        "queue_type=quorum", "durable=true");
    container.execInContainer("rabbitmqadmin", "declare", "binding", "source=" + exchangeName,
        "destination_type=queue", "destination=" + queueName, "routing_key=" + routingKey);
  }

  @AfterAll
  static void shutdownContainer() {
    container.stop();
  }

  @BeforeEach
  void setup() {
    rabbitMQService = new RabbitMQService(MAPPER, processingService, provenanceService,
        rabbitTemplate, new RabbitMQProperties());
  }

  @Test
  void testGetMessages() {
    // Given
    var message = givenMessage();

    // When
    rabbitMQService.getMessages(List.of(message));

    // Then
    then(processingService).should().handleMessages(List.of(givenDigitalSpecimenEvent()));
  }

  @Test
  void testGetInvalidMessages() throws InterruptedException {
    // Given
    var message = givenInvalidMessage();


    // When
    rabbitMQService.getMessages(List.of(message));

    // Then
    var dlqMessage = rabbitTemplate.receive("digital-specimen-queue-dlq");
    assertThat(new String(dlqMessage.getBody())).isEqualTo(message);
    then(processingService).should().handleMessages(List.of());
  }

  @Test
  void testPublishCreateEvent() throws JsonProcessingException, InterruptedException {
    // Given

    // When
    rabbitMQService.publishCreateEvent(givenDigitalSpecimenRecord());

    // Then
    var dlqMessage = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(new String(dlqMessage.getBody())).isNotNull();
  }

  @Test
  void testPublishUpdateEvent() throws JsonProcessingException, InterruptedException {
    // Given

    // When
    rabbitMQService.publishUpdateEvent(givenDigitalSpecimenRecord(2, false), givenJsonPatch());

    // Then
    var dlqMessage = rabbitTemplate.receive("create-update-tombstone-queue");
    assertThat(new String(dlqMessage.getBody())).isNotNull();
  }

  @Test
  void testPublishAnnotationRequestEvent() throws JsonProcessingException, InterruptedException {
    // Given
    var message = givenDigitalSpecimenRecord();

    // When
    rabbitMQService.publishAnnotationRequestEvent(MAS, message);

    // Then
    var result = rabbitTemplate.receive("mas-ocr-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalSpecimenRecord.class)).isEqualTo(
        message);
  }

  @Test
  void testRepublishEvent() throws JsonProcessingException, InterruptedException {
    // Given
    var message = givenDigitalSpecimenEvent();

    // When
    rabbitMQService.republishEvent(message);

    // Then
    var result = rabbitTemplate.receive("digital-specimen-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testDeadLetterEvent() throws JsonProcessingException, InterruptedException {
    // Given
    var message = givenDigitalSpecimenEvent();

    // When
    rabbitMQService.deadLetterEvent(message);

    // Then
    var result = rabbitTemplate.receive("digital-specimen-queue-dlq");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalSpecimenEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testPublishDigitalMediaObjectEvent() throws JsonProcessingException, InterruptedException {
    // Given
    var message = givenDigitalMediaEventWithRelationship();

    // When
    rabbitMQService.publishDigitalMediaObject(message);

    // Then
    var result = rabbitTemplate.receive("digital-media-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), DigitalMediaEvent.class)).isEqualTo(
        message);
  }

  @Test
  void testPublishAcceptedAnnotation() throws JsonProcessingException, InterruptedException {
    // Given
    var message = givenAutoAcceptedAnnotation(givenNewAcceptedAnnotation());

    // When
    rabbitMQService.publishAcceptedAnnotation(message);

    // Then
    var result = rabbitTemplate.receive("auto-annotation-queue");
    assertThat(
        MAPPER.readValue(new String(result.getBody()), AutoAcceptedAnnotation.class)).isEqualTo(
        message);
  }

  private String givenInvalidMessage() {
    return """
        {
          "enrichmentList": [
            "OCR"
          ],
          "digitalSpecimen": {
            "type": "GeologyRockSpecimen",
            "physicalSpecimenID": "https://geocollections.info/specimen/23602",
            "physicalSpecimenIDType": "global",
            "specimenName": "Biota",
            "organisationID": "https://ror.org/0443cwa12",
            "datasetId": null,
            "physicalSpecimenCollection": null,
            "sourceSystemID": "20.5000.1025/MN0-5XP-FFD",
            "data": {},
            "originalData": {},
            "dwcaID": null
          }
        }""";
  }

  private String givenMessage() {
    return """
        {
          "enrichmentList": [
            "OCR"
            ],
          "digitalSpecimenWrapper": {
            "ods:normalisedPhysicalSpecimenID": "https://geocollections.info/specimen/23602",
            "ods:type": "https://doi.org/21.T11148/894b1e6cad57e921764e",
            "ods:attributes": {
              "ods:physicalSpecimenIDType": "Global",
              "ods:physicalSpecimenID":"https://geocollections.info/specimen/23602",
              "ods:organisationID": "https://ror.org/0443cwa12",
              "ods:organisationName": "National Museum of Natural History",
              "ods:normalisedPhysicalSpecimenID": "https://geocollections.info/specimen/23602",
              "ods:specimenName": "Biota",
              "dwc:datasetName": null,
              "dwc:collectionID": null,
              "ods:sourceSystemID": "https://hdl.handle.net/TEST/57Z-6PC-64W",
              "ods:sourceSystemName": "A very nice source system",
              "dcterms:license": "http://creativecommons.org/licenses/by-nc/4.0/",
              "dcterms:modified": "2022-11-01T09:59:24.000Z",
              "ods:topicDiscipline": "Botany",
              "ods:isMarkedAsType": true,
              "ods:isKnownToContainMedia": false,
              "ods:livingOrPreserved": "Preserved"
            },
            "ods:originalAttributes": {
                "abcd:unitID": "152-4972",
                "abcd:sourceID": "GIT",
                "abcd:unitGUID": "https://geocollections.info/specimen/23646",
                "abcd:recordURI": "https://geocollections.info/specimen/23646",
                "abcd:recordBasis": "FossilSpecimen",
                "abcd:unitIDNumeric": 23646,
                "abcd:dateLastEdited": "2004-06-09T10:17:54.000+00:00",
                "abcd:kindOfUnit/0/value": "",
                "abcd:sourceInstitutionID": "Department of Geology, TalTech",
                "abcd:kindOfUnit/0/language": "en",
                "abcd:gathering/country/name/value": "Estonia",
                "abcd:gathering/localityText/value": "Laeva 297 borehole",
                "abcd:gathering/country/iso3166Code": "EE",
                "abcd:gathering/localityText/language": "en",
                "abcd:gathering/altitude/measurementOrFactText/value": "39.9",
                "abcd:identifications/identification/0/preferredFlag": true,
                "abcd:gathering/depth/measurementOrFactAtomised/lowerValue/value": "165",
                "abcd:gathering/depth/measurementOrFactAtomised/unitOfMeasurement": "m",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/spatialDatum": "WGS84",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/term": "Pirgu Stage",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/term": "Katian",
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/latitudeDecimal": 58.489269,
                "abcd:gathering/siteCoordinateSets/siteCoordinates/0/coordinatesLatLong/longitudeDecimal": 26.385719,
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/0/language": "en",
                "abcd:gathering/stratigraphy/chronostratigraphicTerms/chronostratigraphicTerm/1/language": "en",
                "abcd:identifications/identification/0/result/taxonIdentified/scientificName/fullScientificNameString": "Biota",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronostratigraphicName": "Pirgu Stage",
                "abcd-efg:earthScienceSpecimen/unitStratigraphicDetermination/chronostratigraphicAttributions/chronostratigraphicAttribution/0/chronoStratigraphicDivision": "Stage"
              }
          },
          "digitalMediaEvents": []
        }""";
  }
}
