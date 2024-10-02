package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.DIGITAL_MEDIA_OBJECT;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DigitalMediaRepository {

  private final DSLContext context;

  public Map<String, String> getDigitalMediaUrisFromId(List<String> mediaPids) {
    return context.select(DIGITAL_MEDIA_OBJECT.MEDIA_URL, DIGITAL_MEDIA_OBJECT.ID)
        .from(DIGITAL_MEDIA_OBJECT)
        .where(DIGITAL_MEDIA_OBJECT.ID.in(mediaPids))
        .fetchMap(DIGITAL_MEDIA_OBJECT.MEDIA_URL, DIGITAL_MEDIA_OBJECT.ID);
  }

  public void removeSpecimenRelationshipsFromMedia(List<String> mediaPids) {
    context.update(DIGITAL_MEDIA_OBJECT)
        .set(DIGITAL_MEDIA_OBJECT.DIGITAL_SPECIMEN_ID, "UNKNOWN")
        .where(DIGITAL_MEDIA_OBJECT.ID.in(mediaPids))
        .execute();
  }

}
