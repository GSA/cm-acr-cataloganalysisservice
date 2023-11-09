package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.util.ConcurrentModificationException;

import static org.mockito.ArgumentMatchers.any;

@SpringBootTest
@Slf4j
@ContextConfiguration(classes ={XsbDataController.class})
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataControllerTest {

   @MockBean
    private XsbDataService xsbDataService;

    @Autowired
    XsbDataController xsbDataController;

    @Test
    void trigger() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenReturn(Mono.empty());
        StepVerifier.create(xsbDataController.trigger(trigger))
                .expectNext("\nTriggered\n")
                .verifyComplete();
    }

    @Test
    void trigger_alreadyWorking() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenThrow(new ConcurrentModificationException("Working"));
        StepVerifier.create(xsbDataController.trigger(trigger))
                .expectNext("\nWorking\n")
                .verifyComplete();
    }

    @Test
    void trigger_error() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.triggerDataUpload(any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        StepVerifier.create(xsbDataController.trigger(trigger))
                .expectNext("\nTriggered\n")
                .verifyComplete();
    }


    @Test
    void sftp() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.empty());
        StepVerifier.create(xsbDataController.sftp(trigger))
                .verifyComplete();

    }

    @Test
    void sftp_multiple() {
        Path [] dummyFiles = {Path.of("file1"), Path.of("file2")};
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.fromArray(dummyFiles));

        StepVerifier.create(xsbDataController.sftp(trigger))
                .expectNext("file1\n")
                .expectNext("file2\n")
                .verifyComplete();

    }

    @Test
    void sftp_error() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenReturn(Flux.error(new RuntimeException("Dummy")));

        StepVerifier.create(xsbDataController.sftp(trigger))
                .verifyError(RuntimeException.class);

    }

    @Test
    void sftp_exception() {
        Trigger trigger= new Trigger();
        Mockito.when(xsbDataService.downloadReports(any())).thenThrow(new RuntimeException("Dummy Exception"));

        StepVerifier.create(xsbDataController.sftp(trigger))
                .expectNext("Dummy Exception")
                .verifyComplete();
    }

}