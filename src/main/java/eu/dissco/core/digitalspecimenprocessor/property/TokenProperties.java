package eu.dissco.core.digitalspecimenprocessor.property;


import jakarta.annotation.PostConstruct;
import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.validation.annotation.Validated;

@Data
@Validated
@ConfigurationProperties("token")
public class TokenProperties {

    @Value("${auth.secret}")
    private String secret;

    @Value("${auth.id}")
    private String id;

    @Value("${auth.grantType}")
    private String grantType;

    private MultiValueMap<String, String> fromFormData;

    @PostConstruct
    private void setProperties() {
        fromFormData = new LinkedMultiValueMap<>();
        fromFormData.add("grant_type", grantType);
        fromFormData.add("client_id", id);
        fromFormData.add("client_secret", secret);
    }

}
