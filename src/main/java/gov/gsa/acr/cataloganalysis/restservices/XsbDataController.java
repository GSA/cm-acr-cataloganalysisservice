package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
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

    @PostMapping(value="/trigger", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<String> trigger(@RequestBody Trigger trigger){
        try {
            if (trigger.getMonitor()) {
                Sinks.Many<String> statusNotifierSource = Sinks.many().replay().latest();
                Flux<String> statusNotifier = statusNotifierSource.asFlux();
                return statusNotifier.doOnSubscribe(s -> xsbDataService.trigger(trigger, statusNotifierSource));

            }
            else {
                xsbDataService.trigger(trigger, null);
                return Flux.just("Triggered");
            }
        }
        catch (ConcurrentModificationException e){
            return Flux.just(e.getMessage());
        }
    }

    @PostMapping(value="/sftp", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<String> sftp(@RequestBody Trigger trigger){
        if (trigger == null) return Flux.error(new IllegalArgumentException("Invalid request body in POST"));
        log.info("Trigger: " + trigger);
        try {
            if (trigger.getMonitor()) {
                /*Sinks.Many<String> statusNotifierSource = Sinks.many().replay().latest();
                Flux<String> statusNotifier = statusNotifierSource.asFlux();
                return statusNotifier.doOnSubscribe(s -> xsbDataService.downloadReportsFromXSB(trigger, statusNotifierSource));

                 */
                return xsbDataService.downloadReportsFromXSB(trigger, null)
                        .map(String::valueOf)
                        .map(s -> s+"\n")
                        .doOnNext(s -> log.info("File file file " + s));
            }
            else {
                return xsbDataService.downloadReportsFromXSB(trigger, null)
                        .map(String::valueOf)
                        .map(s -> s + "\n")
                        .doOnNext(s -> log.info("File file file " + s));
                //return Flux.just("Triggered");
            }
        }
        catch (ConcurrentModificationException e){
            return Flux.just(e.getMessage());
        }
    }
}
