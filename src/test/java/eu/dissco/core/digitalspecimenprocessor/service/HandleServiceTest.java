package eu.dissco.core.digitalspecimenprocessor.service;

import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.CREATED;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.HANDLE;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.MAPPER;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.PHYSICAL_SPECIMEN_ID;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.SPECIMEN_NAME;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimen;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenEvent;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenDigitalSpecimenRecord;
import static eu.dissco.core.digitalspecimenprocessor.utils.TestUtils.givenUnequalDigitalSpecimenRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.then;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import eu.dissco.core.digitalspecimenprocessor.domain.DigitalSpecimen;
import eu.dissco.core.digitalspecimenprocessor.domain.UpdatedDigitalSpecimenTuple;
import eu.dissco.core.digitalspecimenprocessor.exception.PidCreationException;
import eu.dissco.core.digitalspecimenprocessor.repository.HandleRepository;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerFactory;
import org.jooq.Field;
import org.jooq.Record;
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
  Instant instant;

  @BeforeEach
  void setup() throws ParserConfigurationException {
    var docFactory = DocumentBuilderFactory.newInstance();
    var transFactory = TransformerFactory.newInstance();
    service = new HandleService(random, docFactory.newDocumentBuilder(), repository,
        transFactory);
    Clock clock = Clock.fixed(CREATED, ZoneOffset.UTC);
    instant = Instant.now(clock);
    mockedStatic = mockStatic(Instant.class);
    mockedStatic.when(Instant::now).thenReturn(instant);
  }

  @AfterEach
  void destroy() {
    mockedStatic.close();
  }

  @Test
  void testCreateNewHandle() throws Exception {
    // Given
    given(random.nextInt(33)).willReturn(21);
    mockedStatic.when(() -> Instant.from(any())).thenReturn(instant);

    var expected = "20.5000.1025/YYY-YYY-YYY";

    // When
    var result = service.createNewHandle(givenDigitalSpecimen());

    // Then
    then(repository).should().createHandle(eq(expected), eq(CREATED), anyList());
    assertThat(result).isEqualTo(expected);
  }

  @Test
  void testCreateNewHandleEmptySpecimen() throws Exception {
    // Given
    given(random.nextInt(33)).willReturn(21);
    mockedStatic.when(() -> Instant.from(any())).thenReturn(instant);

    var expected = "20.5000.1025/YYY-YYY-YYY";

    // When
    var result = service.createNewHandle(givenEmptyDigitalSpecimen());

    // Then
    then(repository).should().createHandle(eq(expected), eq(CREATED), anyList());
    assertThat(result).isEqualTo(expected);
  }

  private DigitalSpecimen givenEmptyDigitalSpecimen(){
    return new DigitalSpecimen(PHYSICAL_SPECIMEN_ID, SPECIMEN_NAME, MAPPER.createObjectNode(), MAPPER.createObjectNode());
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

  @Test
  void testCheckForPrimarySpecimenObjectIdIsPresent() {
    // Given
    var mockRecord = mock(Record.class);
    given(mockRecord.get((Field<Object>) any())).willReturn("".getBytes(StandardCharsets.UTF_8));
    var specimen = givenDigitalSpecimen();
    given(repository.searchByPrimarySpecimenObjectId(specimen.physicalSpecimenId().getBytes(
        StandardCharsets.UTF_8))).willReturn(Optional.of(mockRecord));

    // Then
    assertThrows(PidCreationException.class, () ->service.createNewHandle(specimen));
  }
}
