package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.repository.HandleRepository;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Random;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class HandleServiceTest {

  @Mock
  private Random random;
  @Mock
  private HandleRepository repository;
  private MockedStatic<Instant> mockedStatic;

  private HandleService service;

  @BeforeEach
  void setup() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    var transfactory = TransformerFactory.newInstance();
    service = new HandleService(random, MAPPER, docFactory.newDocumentBuilder(), repository,
        transfactory);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    Instant instant = Instant.now(clock);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
  }

  @Test
  void testCreateNewHandle() throws TransformerException {
    // Given
    given(random.nextInt(33)).willReturn(21);
    var expected = "20.5000.1025/YYY-YYY-YYY";

    // When
    var result = service.createNewHandle(givenDigitalSpecimen());

    // Then
    then(repository).should().createHandle(eq(expected), eq(CREATED), anyList());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testUpdateHandle() {
    // Given

    // When
    service.updateHandles(List.of(
        new UpdatedDigitalSpecimenTuple(givenUnequalDigitalSpecimenRecord(),
            givenDigitalSpecimenEvent())));

    // Then
    then(repository).should().updateHandleAttributes(eq(HANDLE), eq(CREATED), anyList(), eq(true));
  }

  @Test
  void testRollbackHandleCreation() {
    // Given

    // When
    service.rollbackHandleCreation(givenDigitalSpecimenRecord());

    // Then
    then(repository).should().rollbackHandleCreation(HANDLE);
  }

  @Test
  void testDeleteVersion() {
    // Given

    // When
    service.deleteVersion(givenDigitalSpecimenRecord());

    // Then
    then(repository).should().updateHandleAttributes(eq(HANDLE), eq(CREATED), anyList(), eq(false));
  }


}
