package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;


/**
 * In order for annotated transactions to work, the @Transactional annotation had to be applied at the Bean level.
 * Previously this annotation was applied to a method within the XsbDataService bean. But the AOP code was not
 * generated or inserted correctly.
 * According to the documentation found at
 * <a href="https://docs.spring.io/spring-framework/reference/data-access/transaction/declarative/annotations.html">...</a>
 * In proxy mode (which is the default), only external method calls coming in through the proxy are intercepted. This
 * means that self-invocation (in effect, a method within the target object calling another method of the target object)
 * does not lead to an actual transaction at runtime even if the invoked method is marked with @Transactional. Also, the
 * proxy must be fully initialized to provide the expected behavior, so you should not rely on this feature in your
 * initialization code (that is, @PostConstruct).
 * Also,
 * The @Transactional annotation is typically used on methods with public visibility. As of 6.0, protected or
 * package-visible methods can also be made transactional for class-based proxies by default. Note that transactional
 * methods in interface-based proxies must always be public and defined in the proxied interface. For both kinds of
 * proxies, only external method calls coming in through the proxy are intercepted.
 */
@Service
@Transactional(rollbackFor = {Exception.class})
@Slf4j
public class TransactionalDataService {
    private final XsbDataRepository xsbDataRepository;
    private final ErrorHandler errorHandler;

    public TransactionalDataService(XsbDataRepository xsbDataRepository, ErrorHandler errorHandler) {
        this.xsbDataRepository = xsbDataRepository;
        this.errorHandler = errorHandler;
    }

    public Mono<Void> moveDataFromStagingToFinal(Trigger trigger) {
        String msg = "Moving data in bulk from staging (xsb_data_temp) table to the final (xsb_data) table.";
        String errMsg = "Error: " + msg;
        try {
            if (trigger == null) throw new NullPointerException("Trigger argument cannot be null.");
            Boolean purgeOldData = trigger.getPurgeOldData();
            // If there are too many errors, do not move the data to the final tables.
            return Mono.just(errorHandler.totalErrorsWithinAcceptableThreshold())
                    .filter(proceed -> proceed)
                    // TBD add functionality if it's a complete replacement or an incremental update
                    .flatMap(proceed -> {
                        if (purgeOldData)
                            return xsbDataRepository.deleteAll()
                                    .doFirst(() -> log.info(msg))
                                    .then(Mono.defer(xsbDataRepository::moveXsbData))
                                    .then(Mono.defer(()-> {
                                        Integer forcedError = trigger.getForcedError();
                                        if (forcedError == 1) {
                                            log.info("Triggering a forced Error 1");
                                            return Mono.error(new Exception("Forced Error 1" ));
                                        }
                                        else if (forcedError == 2) {
                                            log.info("Triggering a forced Error 2");
                                            throw new RuntimeException("Forced Error 2");
                                        }
                                        else return Mono.empty();
                                    }));
                        else
                            return xsbDataRepository.moveXsbData();
                    })
                    .doOnError(e -> {
                        log.error("Mono " + errMsg, e);
                        errorHandler.handleFileError("", errMsg, e);
                        // Convert to RuntimeException so the Transaction fails
                        //throw new RuntimeException(e);
                    });
        } catch (Exception e) {
            log.error("Caught " + errMsg, e);
            errorHandler.handleFileError("", errMsg, e);
            // Convert to RuntimeException so the Transaction fails
            throw new RuntimeException(e);
        }
    }
}
