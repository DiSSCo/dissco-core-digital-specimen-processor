package eu.dissco.core.digitalspecimenprocessor.repository;

import static org.testcontainers.containers.PostgreSQLContainer.IMAGE;

import com.zaxxer.hikari.HikariDataSource;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DefaultDSLContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class BaseRepositoryIT {

  private static final DockerImageName POSTGIS =
      DockerImageName.parse("postgres:13.7").asCompatibleSubstituteFor(IMAGE);

  @Container
  private static final PostgreSQLContainer<?> CONTAINER = new PostgreSQLContainer<>(POSTGIS);
  protected DSLContext context;
  private HikariDataSource dataSource;

  @BeforeEach
  void prepareDatabase() {
    dataSource = new HikariDataSource();
    dataSource.setJdbcUrl(CONTAINER.getJdbcUrl());
    dataSource.setUsername(CONTAINER.getUsername());
    dataSource.setPassword(CONTAINER.getPassword());
    dataSource.setMaximumPoolSize(1);
    dataSource.setConnectionInitSql(CONTAINER.getTestQueryString());
    Flyway.configure().mixed(true).dataSource(dataSource).load().migrate();
    context = new DefaultDSLContext(dataSource, SQLDialect.POSTGRES);
  }

  @AfterEach
  void disposeDataSource() {
    dataSource.close();
  }
}
