package gov.gsa.acr.cataloganalysis.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "xsb.ppapi")
public class PpApiConfig {
    private String hostport;
    private String authUrl;
    private String username;
    private String password;
    private String statsUrl;
}
