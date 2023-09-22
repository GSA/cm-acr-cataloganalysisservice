package gov.gsa.acr.cataloganalysis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.EnrichmentRepository;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@SpringBootApplication
@Slf4j
@EnableR2dbcAuditing
public class CmAcrCataloganalysisserviceApplication {
    final
    EnrichmentRepository enrichmentRepository;
    final XsbDataRepository xsbDataRepository;

    final XsbDataService xsbDataService;

    public CmAcrCataloganalysisserviceApplication(EnrichmentRepository enrichmentRepository,
                                                  XsbDataRepository xsbDataRepository, XsbDataService xsbDataService) {
        this.enrichmentRepository = enrichmentRepository;
        this.xsbDataRepository = xsbDataRepository;
        this.xsbDataService = xsbDataService;
    }

    public static void main(String[] args) {
        SpringApplication.run(CmAcrCataloganalysisserviceApplication.class, args);
    }

    private void close(Closeable closeable){
        try {
            closeable.close();
            log.info("Closed the resource");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Bean
    public CommandLineRunner demo() {
        return (args) -> {
            AtomicInteger counter = new AtomicInteger(0);

            //saveXSBData(3004804,"GS-35F-0119Y").blockLast();

            log.info("XSB Data  found with findAllByContractorNumber: ");
            log.info("=================================================");

            /*try(Stream<Path> files = Files.walk(Paths.get("testData")).filter(Files::isRegularFile)){
                files.forEach(f->log.info(String.valueOf(f)));
            }*/

            Path opPath = Paths.get("testData/errorFile.txt");
            BufferedWriter bw = Files.newBufferedWriter(opPath, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            PrintWriter pw = new PrintWriter(bw);
            /*Flux<Path> files = xsbDataService.xsbResponseFiles(Paths.get("testData"), "gsa");
            xsbDataService.processXSBFiles(files, pw).subscribe(
                            s -> log.info("subscribe: " + String.valueOf(s)),
                            e -> log.error("Something wrong happened", e)
                    );
            */

            Flux.just(1, 2, 3, 4, 5)
                    .doFirst(() -> log.info("three"))
                    .doFirst(() -> log.info("two"))
                    .doFirst(() -> log.info("one"))
                    .doOnNext(a -> log.info("Got " + a))
                    .skip(1)
                    .subscribe(b -> log.info("subscribe " + b));


            String[] header = "FN~|~MI~|~LN".split("\\~\\|\\~", -1);
            String[] values ="Anupam~|~~|~Chandra".split("\\~\\|\\~", -1);

            log.info("Header = " + header[0] + ", " + header[1] + ", " + header[2]);
            log.info("Values = " + values[0] + ", " + values[1] + ", " + values[2]);
            Map<String, String> aMap = IntStream.range(0, header.length)
                    .boxed()
                    .collect(Collectors.toMap(x -> header[x], y -> values[y]));
            log.info("a map = " + aMap);

            ObjectMapper objectMapper = new ObjectMapper();
            try {
                String json = objectMapper.writeValueAsString(aMap);
                log.info(json);
            } catch (JsonProcessingException e) {
                log.error("Map to JSON Error",  e);
            }


            String[] fileNames = { "banana", "testData/fruits", "testData/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa", "testData/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa", "orange"};

            Flux<Path> filesFromList = xsbDataService.xsbResponseFiles(Arrays.asList(fileNames));

            xsbDataService.cleanXsbDataTemp(null)
                    .then(
                            xsbDataService.processXSBFiles(filesFromList, pw)
                                    .onBackpressureBuffer()
                                    .flatMap( x -> xsbDataService.saveXsbDataRecord(x, pw))
                                    .then(xsbDataService.moveXsbData(null))
                                    .doFinally(s -> close(pw))
                    )
                    .subscribe(
                            s -> log.info("subscribe: " + s.toString()),
                            e -> log.error("Umexpected Error", e)
                    );

            //close(pw);
            /*xsbDataRepository.findAllByContractNumber("GS-35F-0119Y", null, 0)
                    .doFirst(() -> counter.set(0))
                    .doOnNext(x -> {if(counter.incrementAndGet() % 1000 ==0) log.info(x.toString());})
                    .doOnComplete(() -> log.info("================================================="))
                    .subscribe();

             */
        };
    }


    @Transactional
    public Flux<XsbData> saveXSBData(Integer transaction_id, String contractNumber) {
        AtomicInteger counter = new AtomicInteger(0);

        xsbDataRepository.deleteAllByContractNumber(contractNumber).block();

        log.info("Enrichment found with findAllByTransactionId: ");
        log.info("----------------------------------------------");
        return enrichmentRepository
                .findAllByTransactionId(transaction_id, null, 0)
                .doFirst(() -> counter.set(0))
                .doOnNext(e -> {
                    if(counter.incrementAndGet() % 1000 ==0) log.info("Processed {} records", counter.get());
                })
                .map(Enrichment::toXsbData)
                .flatMap(xsbDataRepository::save)
                .doOnError(e -> {throw new RuntimeException(e);}) //Rethrow as a RuntimeException to make the transaction fail
                .doOnComplete(()->log.info("Saved {} records", counter.get()));
    }

}
