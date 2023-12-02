package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(ErrorHandler.class), @MockBean(XsbDataRepository.class)})
@ContextConfiguration(classes = {S3ClientConfiguration.class, TransactionalDataService.class})
class TransactionalDataServiceTest {
    @Autowired
    private XsbDataRepository xsbDataRepository;
    @Autowired
    private TransactionalDataService transactionalDataService;


    @Test
    void testReplace_deleteAllFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_deleteAllFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.replace())
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testReplace_deleteAllFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
    }

    @Test
    void testReplace_deleteAllFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_deleteAllFailed");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.replace())
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
    }

    @Test
    void testReplace_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        StepVerifier.create(transactionalDataService.replace())
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }

    @Test
    void testReplace_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.replace())
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }

    @Test
    void testUpdate_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.update())
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testReplace_MoveFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }

    @Test
    void testUpdate_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.update())
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }

    @Test
    void testUpdate() {
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();

    }

    @Test
    void testReplace() {
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.replace())
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();

    }

    @Test
    void testTestRollbackUpdate() {
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.testRollbackUpdate())
                .expectError(Exception.class)
                .verify();
        StepVerifier.create(transactionalDataService.testRollbackUpdate())
                .expectErrorMessage("Update Forced Error")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData();
    }

    @Test
    void testTestRollbackUpdate_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.testRollbackUpdate())
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testReplace_MoveFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }

    @Test
    void testTestRollbackUpdate_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.testRollbackUpdate())
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData();
    }


    @Test
    void testTestRollbackReplace() {
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectError(Exception.class)
                .verify();
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectErrorMessage("Replace Forced Error")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData();
    }

    @Test
    void testTestRollbackReplace_DeleteAllFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testTestRollbackReplace_DeleteAllFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testTestRollbackReplace_DeleteAllFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
    }

    @Test
    void testTestRollbackReplace_DeleteAllFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testTestRollbackReplace_DeleteAllFailed");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectErrorMessage("Dummy message from testTestRollbackReplace_DeleteAllFailed")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData();
    }



    @Test
    void testTestRollbackReplace_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenThrow(e);
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectErrorMessage("Dummy message from testReplace_MoveFailedThrowsException")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData();
    }

    @Test
    void testTestRollbackReplace_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.testRollbackReplace())
                .expectErrorMessage("Dummy message from testReplace_MoveFailedThrowsException")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData();
    }

}