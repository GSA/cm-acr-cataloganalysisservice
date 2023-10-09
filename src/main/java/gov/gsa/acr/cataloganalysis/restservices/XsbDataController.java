package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ConcurrentModificationException;

@Slf4j
@RestController
@Tag(name= "ACR Catalog Analysis Service", description = "A Service for analyzing catalogs.")
public class XsbDataController extends BaseController{
    final
    XsbDataService xsbDataService;

    public XsbDataController(XsbDataService xsbDataService) {
        this.xsbDataService = xsbDataService;
    }

    // TBD: Mark for Deletion
    @Operation(summary = "Read products from enrichment table and save it to xsb_data table.",
            description = "This operation reads data from the  enrichment table and saves ti to the xsb_data table given a transaction ID and contract number."
    )
    @GetMapping(value ="/xsb-data/{txnId}/{contractNumber}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<XsbData> xsb(@Parameter(description = "The transaction ID")
                                @PathVariable Integer txnId,
                             @Parameter(description = "The Contract Number") @PathVariable String contractNumber){
        return xsbDataService.saveXSBData(txnId, contractNumber);
    }

    @Operation(summary = "Read products from enrichment table and save it to xsb_data table.",
            description = "This operation reads data from the  enrichment table and saves ti to the xsb_data table given a transaction ID and contract number."
    )
    @GetMapping(value ="/xsb-data-temp/{txnId}", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> xsbDataTemp(@Parameter(description = "The transaction ID") @PathVariable Integer txnId){
        Sinks.Many<String> statusNotifierSource = Sinks.many().replay().latest();
        Flux<String> statusNotifier = statusNotifierSource.asFlux();
        return statusNotifier
                .doOnSubscribe(s-> {
                    xsbDataService
                            .saveXSBDataTemp (txnId, statusNotifierSource)
                            .then(xsbDataService.moveXsbData(statusNotifierSource))
                            .doOnSuccess(x -> {
                                log.info("Completed the entire data save pipeline");
                                statusNotifierSource.tryEmitNext("Completed the entire data save pipeline");
                                statusNotifierSource.tryEmitComplete();
                            })
                            .subscribe();
                });
    }


    @Operation(summary = "Get Enriched records.",
            description = "This operation gets enriched records from the enrichment table given a transaction ID ."
    )
    @GetMapping(value ="/enrichment/{txnId}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<Enrichment> enrichmentFlux(@Parameter(description = "The transaction ID")
                             @PathVariable Integer txnId){
        return xsbDataService.getEnrichment(txnId)
                .doOnNext(e->log.info("here"))
                .doOnComplete(() -> log.info("yay"));
    }


    @PostMapping(value="/trigger", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<String> trigger(@RequestBody Trigger trigger){
        log.info("Triggered " + trigger);
        try {
            xsbDataService.trigger(trigger);
            return Mono.just("Triggered");
        }
        catch (ConcurrentModificationException e){
            return Mono.just(e.getMessage());
        }

    }
}
