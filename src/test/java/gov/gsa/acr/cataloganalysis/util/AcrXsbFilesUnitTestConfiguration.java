package gov.gsa.acr.cataloganalysis.util;

import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AcrXsbFilesUnitTestConfiguration {
    @Bean
    @Primary
    public ErrorHandler errorHandler(){return Mockito.mock(ErrorHandler.class);}
}
