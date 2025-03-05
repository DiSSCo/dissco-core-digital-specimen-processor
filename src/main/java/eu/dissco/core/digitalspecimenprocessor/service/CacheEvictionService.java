package eu.dissco.core.digitalspecimenprocessor.service;

import java.util.Objects;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.CacheManager;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CacheEvictionService {

  private final CacheManager cacheManager;

  @Scheduled(fixedRateString = "PT30M")
  public void evictAllCaches() {
    log.info("Evicting all caches");
    cacheManager.getCacheNames()
        .forEach(cacheName -> Objects.requireNonNull(cacheManager.getCache(cacheName)).clear());
  }
}
