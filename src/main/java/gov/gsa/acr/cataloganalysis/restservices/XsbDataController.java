package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

import java.util.ConcurrentModificationException;

@Slf4j
@RestController
@Tag(name= "ACR Catalog Analysis Service", description = "A Service for analyzing catalogs.")
public class XsbDataController extends BaseController{
    final XsbDataService xsbDataService;

    public XsbDataController(XsbDataService xsbDataService) {
        this.xsbDataService = xsbDataService;
    }

    @PostMapping(value="/trigger", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<String> trigger(@RequestBody Trigger trigger){
        log.info("Request body " + trigger);
        String message;
        try {
            xsbDataService.trigger(trigger);
            message = "\nTriggered\n";
        }
        catch (ConcurrentModificationException e){
            message = "\n"+e.getMessage()+"\n";
        }
        return Mono.just(message);
    }
}
