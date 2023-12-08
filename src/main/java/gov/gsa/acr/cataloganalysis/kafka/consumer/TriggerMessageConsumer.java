package gov.gsa.acr.cataloganalysis.kafka.consumer;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class TriggerMessageConsumer {

    @KafkaListener(topics = "${spring.kafka.topic.trigger.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "triggerKafkaListenerContainerFactory",
            autoStartup = "${spring.kafka.listener.auto-startup}"
    )
    public void listenForTrigger(@Payload Trigger triggerMsg) {
        log.info("MESSAGE RECEIVED: " + triggerMsg );
    }
}
