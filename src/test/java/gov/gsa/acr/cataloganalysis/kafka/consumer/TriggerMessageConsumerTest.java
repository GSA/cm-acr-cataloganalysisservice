package gov.gsa.acr.cataloganalysis.kafka.consumer;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import gov.gsa.acr.cataloganalysis.configuration.KafkaConsumerConfig;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Mono;

import java.util.ConcurrentModificationException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
@EnableKafka
@EmbeddedKafka(partitions = 1,
        controlledShutdown = false,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:3333",
                "port=3333"
        })
@ActiveProfiles("junit")
@Slf4j
@ContextConfiguration(classes = {KafkaConsumerConfig.class, TriggerMessageConsumer.class})
@MockBeans({@MockBean(AnalysisDataProcessingService.class)})
class TriggerMessageConsumerTest {

    @Value("${spring.kafka.topic.trigger.name}")
    private String topic;


    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;


    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;


    @Autowired
    private TriggerMessageConsumer triggerMessageConsumer;

    private ListAppender<ILoggingEvent> logWatcher;

    @BeforeEach
    void setUp() {
        logWatcher = new ListAppender<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger(TriggerMessageConsumer.class)).addAppender(logWatcher);
    }

    @AfterEach
    void tearDown() {
        ((Logger) LoggerFactory.getLogger(TriggerMessageConsumer.class)).detachAndStopAllAppenders();
    }


    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
    }


    @Test
    void listenForTrigger() throws InterruptedException {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();
        int logSize = logWatcher.list.size();
        assertEquals(1, logSize);
        assertEquals("Trigger Message: Trigger(sourceType=XSB, sourceFolder=null, files=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], purgeOldData=true, uniqueFileNames=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], forcedError=0)", logWatcher.list.get(0).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.INFO, logWatcher.list.get(0).getLevel());
        Mockito.verify(analysisDataProcessingService, Mockito.times(1)).triggerDataUpload(any());
    }

    @Test
    void triggerReturnsError() throws InterruptedException {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();
        int logSize = logWatcher.list.size();
        assertEquals(2, logSize);
        assertEquals("Trigger Message: Trigger(sourceType=XSB, sourceFolder=null, files=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], purgeOldData=true, uniqueFileNames=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], forcedError=0)", logWatcher.list.get(0).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.INFO, logWatcher.list.get(0).getLevel());
        assertEquals("Unexpected Error", logWatcher.list.get(1).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.ERROR, logWatcher.list.get(1).getLevel());
        Mockito.verify(analysisDataProcessingService, Mockito.times(1)).triggerDataUpload(any());
    }



    @Test
    void invalidMessage() throws InterruptedException {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"INVALID\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();
        Mockito.verify(analysisDataProcessingService, Mockito.never()).triggerDataUpload(any());
    }

    @Test
    void testProcessAlreadyExecuting() throws InterruptedException {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(ConcurrentModificationException.class);
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();
        int logSize = logWatcher.list.size();
        assertEquals(2, logSize);
        assertEquals("Trigger Message: Trigger(sourceType=XSB, sourceFolder=null, files=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], purgeOldData=true, uniqueFileNames=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], forcedError=0)", logWatcher.list.get(0).getFormattedMessage());
        assertEquals("Process already executing", logWatcher.list.get(1).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.ERROR, logWatcher.list.get(1).getLevel());
        Mockito.verify(analysisDataProcessingService, Mockito.times(1)).triggerDataUpload(any());
    }


    @Test
    void testIllegalTriggerObject() throws InterruptedException {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(IllegalArgumentException.class);
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();
        int logSize = logWatcher.list.size();
        assertEquals(2, logSize);
        assertEquals("Trigger Message: Trigger(sourceType=XSB, sourceFolder=null, files=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], purgeOldData=true, uniqueFileNames=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], forcedError=0)", logWatcher.list.get(0).getFormattedMessage());
        assertEquals("The request is illegal.", logWatcher.list.get(1).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.ERROR, logWatcher.list.get(1).getLevel());
        Mockito.verify(analysisDataProcessingService, Mockito.times(1)).triggerDataUpload(any());
    }


    @Test
    void testGeneralFailure() throws InterruptedException {
        RuntimeException r = new RuntimeException("Dummy RuntimeException from testGeneralFailure");
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(r);
        Thread.sleep(3000);
        KafkaTemplate<String, String> kafkaTemplate = new KafkaTemplate<>(producerFactory());
        kafkaTemplate.setDefaultTopic(topic);

        String data = "{\"sourceType\": \"XSB\",  \"files\":[\"GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa\"]}";

        kafkaTemplate.send(topic, data);
        Thread.sleep(2000);
        kafkaTemplate.destroy();

        int logSize = logWatcher.list.size();
        assertEquals(2, logSize);
        assertEquals("Trigger Message: Trigger(sourceType=XSB, sourceFolder=null, files=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], purgeOldData=true, uniqueFileNames=[GS-35F-0431Y-3001981_20230727114706_5597566526439952371_report_1.fsa], forcedError=0)", logWatcher.list.get(0).getFormattedMessage());
        assertEquals("Unexpected error", logWatcher.list.get(1).getFormattedMessage());
        assertEquals(ch.qos.logback.classic.Level.ERROR, logWatcher.list.get(1).getLevel());
        Mockito.verify(analysisDataProcessingService, Mockito.times(1)).triggerDataUpload(any());
    }

}