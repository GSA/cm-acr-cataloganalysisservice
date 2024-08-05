package gov.gsa.acr.cataloganalysis;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;

@SpringBootApplication
@Slf4j
@EnableR2dbcAuditing
public class CmAcrCataloganalysisserviceApplication {
    public CmAcrCataloganalysisserviceApplication() {}

    public static void main(String[] args) {
        SpringApplication.run(CmAcrCataloganalysisserviceApplication.class, args);
    }
}
