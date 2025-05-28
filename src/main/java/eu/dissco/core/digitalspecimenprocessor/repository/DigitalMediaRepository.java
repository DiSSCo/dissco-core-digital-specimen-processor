package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import eu.dissco.core.digitalspecimenprocessor.domain.media.DigitalMediaRecord;
import eu.dissco.core.digitalspecimenprocessor.schema.DigitalMedia;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.JSONB;
import org.jooq.Query;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
@Slf4j
public class DigitalMediaRepository {

  private final DSLContext context;
  private final ObjectMapper mapper;

  // Maps Media URI to its DOI
  public List<DigitalMediaRecord> getExistingDigitalMedia(Set<String> mediaURIs) {
    return context.select(DIGITAL_MEDIA_OBJECT.asterisk())
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.MEDIA_URL.in(mediaURIs))
        .fetch(this::mapDigitalMedia);
  }

  public void rollBackDigitalMedia(String id) {
    context.delete(DIGITAL_MEDIA_OBJECT).where(DIGITAL_MEDIA_OBJECT.ID.eq(id)).execute();
  }

  private DigitalMediaRecord mapDigitalMedia(Record dbRecord) {
    try {
      return new DigitalMediaRecord(
          dbRecord.get(DIGITAL_MEDIA_OBJECT.ID),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.MEDIA_URL),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.VERSION),
          dbRecord.get(DIGITAL_MEDIA_OBJECT.CREATED),
          List.of(),
          mapper.readValue(dbRecord.get(DIGITAL_MEDIA_OBJECT.DATA).data(), DigitalMedia.class),
          mapper.readTree(dbRecord.get(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA).data()));
    } catch (JsonProcessingException e) {
      log.error("Unable to map record data to json: {}", dbRecord);
      return null;
    }
  }

  public void updateLastChecked(List<String> currentDigitalMedia) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .where(DIGITAL_MEDIA_OBJECT.ID.in(currentDigitalMedia))
        .execute();
  }

  public int[] createDigitalMediaRecord(
      Set<DigitalMediaRecord> digitalMediaRecords) {
    var queries = digitalMediaRecords.stream().map(this::digitalMediaToQuery).toList();
    return context.batch(queries).execute();
  }

  public Query digitalMediaToQuery(DigitalMediaRecord digitalMediaRecord) {
    return context.insertInto(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.ID, digitalMediaRecord.id())
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.attributes().getOdsFdoType())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.ORIGINAL_DATA,
            JSONB.jsonb(
                digitalMediaRecord.originalAttributes().toString()))
        .set(DIGITAL_MEDIA_OBJECT.MODIFIED, Instant.now())
        .onConflict(DIGITAL_MEDIA_OBJECT.ID).doUpdate()
        .set(DIGITAL_MEDIA_OBJECT.TYPE, digitalMediaRecord.attributes().getOdsFdoType())
        .set(DIGITAL_MEDIA_OBJECT.VERSION, digitalMediaRecord.version())
        .set(DIGITAL_MEDIA_OBJECT.MEDIA_URL,
            digitalMediaRecord.attributes().getAcAccessURI())
        .set(DIGITAL_MEDIA_OBJECT.CREATED, digitalMediaRecord.created())
        .set(DIGITAL_MEDIA_OBJECT.LAST_CHECKED, Instant.now())
        .set(DIGITAL_MEDIA_OBJECT.DATA,
            JSONB.jsonb(
                mapper.valueToTree(digitalMediaRecord.attributes())
                    .toString()))
        .set(DIGITAL_MEDIA_OBJECT.MODIFIED, Instant.now());
  }

}
