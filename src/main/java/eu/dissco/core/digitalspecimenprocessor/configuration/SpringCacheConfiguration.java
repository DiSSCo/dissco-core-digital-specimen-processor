package eu.dissco.core.digitalspecimenprocessor.configuration;

import org.springframework.cache.CacheManager;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SpringCacheConfiguration {

  @Bean
  public CacheManager cacheManager() {
    return new ConcurrentMapCacheManager("source-system-name");
  }
}
