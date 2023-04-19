package eu.dissco.core.digitalspecimenprocessor.security;

import jakarta.validation.constraints.NotNull;
import java.util.Collection;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.springframework.core.convert.converter.Converter;
import org.springframework.security.authentication.AbstractAuthenticationToken;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.springframework.security.oauth2.server.resource.authentication.JwtGrantedAuthoritiesConverter;
import org.springframework.stereotype.Component;

@Component
public class JwtAuthConverter implements Converter<Jwt, AbstractAuthenticationToken> {
  private final JwtGrantedAuthoritiesConverter jwtGrantedAuthoritiesConverter = new JwtGrantedAuthoritiesConverter();

  @Override
  public AbstractAuthenticationToken convert(@NotNull Jwt jwt) {
    Collection<GrantedAuthority> authorities =
        converterToStream(jwt).collect(Collectors.toSet());
    return new JwtAuthenticationToken(jwt, authorities, getPrincipalClaimName(jwt));
  }

  private Stream<GrantedAuthority> converterToStream(Jwt jwt){
    return Optional.of(jwtGrantedAuthoritiesConverter.convert(jwt)).stream()
        .flatMap(Collection::stream);
  }

  private String getPrincipalClaimName(Jwt jwt) {
    return jwt.getClaim(JwtClaimNames.SUB);
  }

}
