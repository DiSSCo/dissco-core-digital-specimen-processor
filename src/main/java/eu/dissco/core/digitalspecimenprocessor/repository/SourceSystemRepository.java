package eu.dissco.core.digitalspecimenprocessor.repository;

import static eu.dissco.core.digitalspecimenprocessor.database.jooq.Tables.SOURCE_SYSTEM;

import lombok.RequiredArgsConstructor;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class SourceSystemRepository {

  private final DSLContext context;

  public String retrieveNameByID(String id) {
    return context.selectFrom(SOURCE_SYSTEM)
        .where(SOURCE_SYSTEM.ID.eq(id))
        .fetchOne(SOURCE_SYSTEM.NAME);
  }
}
