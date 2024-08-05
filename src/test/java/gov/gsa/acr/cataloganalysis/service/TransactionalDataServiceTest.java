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
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.replace(1))
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testReplace_deleteAllFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
    }

    @Test
    void testReplace_deleteAllFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_deleteAllFailed");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.replace(20))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
    }

    @Test
    void testReplace_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());

        when(xsbDataRepository.moveXsbData_0()).thenThrow(e);
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());

        StepVerifier.create(transactionalDataService.replace(20))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_19();
    }

    @Test
    void testReplace_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.replace(1))
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
    }

    @Test
    void testUpdate_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenThrow(e);
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(20))
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.update(20))
                .expectErrorMessage("java.lang.reflect.InvocationTargetException")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_19();

    }

    @Test
    void testUpdate_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.update(20))
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();
    }


    @Test
    void testReplace() {
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.replace(1))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();

    }

    @Test
    void testTestRollbackUpdate() {
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(new Exception("Update Forced Error")));
        StepVerifier.create(transactionalDataService.update(1))
                .expectError(Exception.class)
                .verify();
        StepVerifier.create(transactionalDataService.update(1))
                .expectErrorMessage("Update Forced Error")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_0();
    }

    @Test
    void testTestRollbackUpdate_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData_0()).thenThrow(e);
        StepVerifier.create(transactionalDataService.update(1))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
    }

    @Test
    void testTestRollbackUpdate_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.update(1))
                .expectError(RuntimeException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.never()).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
    }


    @Test
    void testTestRollbackReplace() {
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(new Exception ("Replace Forced Error")));
        StepVerifier.create(transactionalDataService.replace(1))
                .expectError(Exception.class)
                .verify();
        StepVerifier.create(transactionalDataService.replace(1))
                .expectErrorMessage("Replace Forced Error")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
    }

    @Test
    void testTestRollbackReplace_DeleteAllFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testTestRollbackReplace_DeleteAllFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenThrow(e);
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        IllegalArgumentException thrown = assertThrows(IllegalArgumentException.class, () -> StepVerifier.create(transactionalDataService.replace(1))
                .expectError(IllegalArgumentException.class)
                .verify());
        assertEquals("Dummy message from testTestRollbackReplace_DeleteAllFailedThrowsException", thrown.getMessage());
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
    }

    @Test
    void testTestRollbackReplace_DeleteAllFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testTestRollbackReplace_DeleteAllFailed");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.error(e));
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.replace(1))
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.replace(1))
                .expectErrorMessage("Dummy message from testTestRollbackReplace_DeleteAllFailed")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_0();
    }



    @Test
    void testTestRollbackReplace_MoveFailedThrowsException() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenThrow(e);
        StepVerifier.create(transactionalDataService.replace(1))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
    }

    @Test
    void testTestRollbackReplace_MoveFailed() {
        Exception e = new IllegalArgumentException("Dummy message from testReplace_MoveFailedThrowsException");
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.error(e));
        StepVerifier.create(transactionalDataService.replace(1))
                .expectError(IllegalArgumentException.class)
                .verify();
        StepVerifier.create(transactionalDataService.replace(1))
                .expectErrorMessage("Dummy message from testReplace_MoveFailedThrowsException")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).deleteAll();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.never()).moveXsbData_1();
    }

    @Test
    void testUpdate_ThreePartitions(){
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(3))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_19();

    }

    @Test
    void testUpdate_TwentyPartitions(){
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(20))
                .verifyComplete();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

    }


    @Test
    void testUpdate_InvalidNumberOfPartitions(){
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(22))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_19();

    }


    @Test
    void testUpdate_moveFailed(){
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenThrow(new RuntimeException("Dummy"));
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(20))
                .expectError(IllegalArgumentException.class)
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(1)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_19();

    }


    @Test
    void testUpdate_MoveHadAnError(){
        when(xsbDataRepository.moveXsbData_0()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_1()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_2()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_3()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_4()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_5()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_6()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_7()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_8()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_9()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_10()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_11()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_12()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_13()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_14()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_15()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_16()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_17()).thenReturn(Mono.error(new RuntimeException("testUpdate_InvalidNumberOfPartitions")));
        when(xsbDataRepository.moveXsbData_18()).thenReturn(Mono.empty());
        when(xsbDataRepository.moveXsbData_19()).thenReturn(Mono.empty());
        StepVerifier.create(transactionalDataService.update(20))
                .expectError(RuntimeException.class)
                .verify();
        StepVerifier.create(transactionalDataService.update(20))
                .expectErrorMessage("testUpdate_InvalidNumberOfPartitions")
                .verify();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_0();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_1();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_2();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_3();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_4();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_5();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_6();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_7();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_8();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_9();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_10();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_11();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_12();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_13();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_14();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_15();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_16();
        Mockito.verify(xsbDataRepository, Mockito.times(2)).moveXsbData_17();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_18();
        Mockito.verify(xsbDataRepository, Mockito.times(0)).moveXsbData_19();

    }

}