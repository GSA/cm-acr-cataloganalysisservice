package gov.gsa.acr.cataloganalysis.configuration;

import io.swagger.v3.oas.models.Components;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.security.SecurityRequirement;
import io.swagger.v3.oas.models.security.SecurityScheme;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class SwaggerConfig {

    @Bean
    public OpenAPI streamOpenApi(@Value("${springdoc.version}") String appVersion,
                                 @Value("${springdoc.pathsToMatch}") String pathsToMatch,
                                 @Value("${springdoc.packagesToScan}") String packagesToScan
    ) {

        return new OpenAPI().addSecurityItem(new SecurityRequirement().
                        addList("bearer-key"))
                .components(new Components().addSecuritySchemes
                        ("bearer-key", createAPIKeyScheme()))
                .info(new Info().title("ACR Data Streaming API")
                        .description("This microservice is for streaming data.")
                        .version("v1")
                        .contact(new Contact().name("General Services Administration")))
                .tags(Arrays.asList());
    }


    private SecurityScheme createAPIKeyScheme() {
        return new SecurityScheme().type(SecurityScheme.Type.HTTP)
                .bearerFormat("JWT")
                .scheme("bearer");
    }
}
