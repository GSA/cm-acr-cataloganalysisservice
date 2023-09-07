package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@Slf4j
@RestController
public class XsbDataController extends BaseController{
    final
    XsbDataService xsbDataService;

    public XsbDataController(XsbDataService xsbDataService) {
        this.xsbDataService = xsbDataService;
    }

    @GetMapping(value ="/xsb-data/{txnId}/{contractNumber}", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<String> xsb(@PathVariable Integer txnId, @PathVariable String contractNumber){
        xsbDataService.saveXSBData(txnId, contractNumber).subscribe();
        return Mono.just("Saving transaction ID " + txnId + " and contract " + contractNumber);
    }

}
