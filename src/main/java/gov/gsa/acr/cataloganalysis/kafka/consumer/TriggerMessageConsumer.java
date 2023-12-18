package gov.gsa.acr.cataloganalysis.kafka.consumer;

import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.util.ConcurrentModificationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Slf4j
public class TriggerMessageConsumer {
    final
    AnalysisDataProcessingService analysisDataProcessingService;

    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    public TriggerMessageConsumer(AnalysisDataProcessingService analysisDataProcessingService) {
        this.analysisDataProcessingService = analysisDataProcessingService;
    }

    @KafkaListener(
            topics = "${spring.kafka.topic.trigger.name}"
            , groupId = "${spring.kafka.consumer.group-id}"
            , containerFactory = "triggerKafkaListenerContainerFactory"
            , autoStartup = "${spring.kafka.listener.auto-startup}"
    )
    public void listenForTrigger(@Payload Trigger trigger) {
        log.info("Trigger Message: " + trigger);
        try {
            Mono<DataUploadResults> dataUploadResultsMono =  analysisDataProcessingService.triggerDataUpload(trigger);
            executorService.submit(() -> dataUploadResultsMono.subscribe(null, e -> log.error("Unexpected Error", e)));
        }
        catch (ConcurrentModificationException e){
            log.error("Process already executing", e);
        }
        catch (IllegalArgumentException e){
            log.error("The request is illegal.", e);
        }
        catch (Exception e) {
            log.error("Unexpected error", e);
        }
    }
}
