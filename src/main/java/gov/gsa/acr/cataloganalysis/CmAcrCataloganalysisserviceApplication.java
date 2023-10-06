package gov.gsa.acr.cataloganalysis;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.EnrichmentRepository;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.service.ErrorHandler;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.data.r2dbc.config.EnableR2dbcAuditing;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

@SpringBootApplication
@Slf4j
@EnableR2dbcAuditing
public class CmAcrCataloganalysisserviceApplication {
    final
    EnrichmentRepository enrichmentRepository;
    final XsbDataRepository xsbDataRepository;

    final XsbDataService xsbDataService;

    final
    ErrorHandler errorHandler;

    public CmAcrCataloganalysisserviceApplication(EnrichmentRepository enrichmentRepository,
                                                  XsbDataRepository xsbDataRepository, XsbDataService xsbDataService, ErrorHandler errorHandler) {
        this.enrichmentRepository = enrichmentRepository;
        this.xsbDataRepository = xsbDataRepository;
        this.xsbDataService = xsbDataService;
        this.errorHandler = errorHandler;
    }

    public static void main(String[] args) {
        SpringApplication.run(CmAcrCataloganalysisserviceApplication.class, args);
    }


    @Bean
    public CommandLineRunner demo() {
        return (args) -> {
            errorHandler.init();


//            String[] fileNames = { "banana", "testData/fruits", "testData/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa", "testData/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa"
//                    , "testData/GS-06F-0052R-3008634_20230816153812_6606792615789196106_report_1.gsa"
//                    , "orange", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"};

            String[] fileNames = { "testData/testFileWithErrors.gsa", "banana", "testData/z1.gsa", "testData/z2.gsa"};


            AtomicInteger dbCounter = new AtomicInteger(0);

            Flux<Path> filesFromList = xsbDataService.xsbResponseFiles(Arrays.asList(fileNames));
            //Flux<Path> filesFromList = xsbDataService.xsbResponseFiles(Paths.get("testData1"), "gsa");

            xsbDataService.cleanXsbDataTemp(null)
                    .then(
                            xsbDataRepository
                                    .findTaaCompliantCountries()
                                    .collectList()
                                    .doOnError(e -> log.error("Unable to get a list of TAA compliant country codes. Exiting!", e))
                                    .onErrorStop()
                                    .flatMapMany(taaCountryCodes ->
                                                    xsbDataService.processXSBFiles(filesFromList, errorHandler, taaCountryCodes)
                                    )
                                    .onBackpressureBuffer()
                                    .flatMap( x -> xsbDataService.saveXsbDataRecord(x, errorHandler))
                                    .doFirst(() -> dbCounter.set(0))
                                    .doOnNext(e -> {
                                        if (dbCounter.incrementAndGet() % 1000 == 0) {
                                            log.info("Saved {} records", dbCounter.get());
                                        }
                                    })
                                    .doOnComplete(() -> {
                                        log.info("Finished. Saved a total of {} records", dbCounter.get());
                                        log.info("Number of parsing errors: " +errorHandler.getNumParsingErrors().get());
                                        log.info("Number of db errors: " +errorHandler.getNumDbErrors().get());
                                        log.info("Number of file errors: " +errorHandler.getNumFileErrors().get());
                                    })
                                    .then(xsbDataService.moveXsbData(null))
                                    .doFinally(s -> errorHandler.close())
                    )
                    .subscribe(
                            s -> log.info("subscribe: " + s.toString()),
                            e -> log.error("Unexpected Error", e)
                    );

        };
    }

}
