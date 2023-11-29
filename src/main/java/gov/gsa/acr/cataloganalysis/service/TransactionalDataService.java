package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Mono;

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
