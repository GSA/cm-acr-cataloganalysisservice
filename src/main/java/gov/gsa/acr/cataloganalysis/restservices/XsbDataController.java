package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Enrichment;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@Slf4j
@RestController
@Tag(name= "ACR Catalog Analysis Service", description = "A Service for analyzing catalogs.")
public class XsbDataController extends BaseController{
    final
    XsbDataService xsbDataService;

    public XsbDataController(XsbDataService xsbDataService) {
        this.xsbDataService = xsbDataService;
    }

    @Operation(summary = "Move products from enrichment table to xsb_data table.",
            description = "This operation moves data from enrichment to the xsb_data table given a transaction ID and contract number."
    )
    @GetMapping(value ="/xsb-data/{txnId}/{contractNumber}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<XsbData> xsb(@Parameter(description = "The transaction ID")
                                @PathVariable Integer txnId,
                             @Parameter(description = "The Contract Number") @PathVariable String contractNumber){
        return xsbDataService.saveXSBData(txnId, contractNumber);
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
}
