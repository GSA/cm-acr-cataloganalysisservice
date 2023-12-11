package gov.gsa.acr.cataloganalysis.kafka.consumer;

import gov.gsa.acr.cataloganalysis.configuration.KafkaConsumerConfig;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
/*@EnableKafka
@EmbeddedKafka(partitions = 1,
        controlledShutdown = false,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:3333",
                "port=3333"
        })

 */
@ActiveProfiles("junit")
@Slf4j
//@ContextConfiguration(classes = {KafkaConsumerConfig.class, TriggerMessageConsumer.class})
class TriggerMessageConsumerTest {

    @Value("${spring.kafka.topic.trigger.name}")
    private String topic;

/*
    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;




    @Autowired
    private TriggerMessageConsumer triggerMessageConsumer;
*/
    @BeforeAll
    static void init() {
        log.info("Before All");
    }

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    /*
    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }

     */

    @Test
    void listenForTrigger() throws InterruptedException {
       /* KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        log.info("Reached here");
        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.gsa\"]}";
        String brokerList = embeddedKafkaBroker.getBrokersAsString();
        log.info("Anupam Chandra " + brokerList);
        kafkaTemplate.send(topic, data);
        Thread.sleep(3000);
        //log.info("Payload that consumer got: " + triggerMessageConsumer.get)

        */

    }
}