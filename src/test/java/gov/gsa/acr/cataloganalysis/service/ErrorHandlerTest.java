package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.util.AcrXsbFilesUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {ErrorHandler.class})
@TestPropertySource(locations="classpath:application-test.properties")
class ErrorHandlerTest {
    @Autowired
    private ErrorHandler errorHandler;

    private boolean isEmpty(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (DirectoryStream<Path> directory = Files.newDirectoryStream(path)) {
                return !directory.iterator().hasNext();
            }
        }
        return false;
    }

    private String getAlphaNumericString(int n) {
        // choose a Character random from this String
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZ" + "0123456789" + "abcdefghijklmnopqrstuvxyz";

        // create StringBuffer size of AlphaNumericString
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) {
            // generate a random number between
            // 0 to AlphaNumericString variable length
            int index = (int) (AlphaNumericString.length() * Math.random());
            // add Character one by one in end of sb
            sb.append(AlphaNumericString.charAt(index));
        }
        return sb.toString();
    }


    @BeforeEach
    void setUp() {
        errorHandler.init("dummy header");
    }

    @Test
    void init() throws IOException {
        assertEquals(2000, errorHandler.maxErrorFileSizeBytes);
        assertEquals("testData/errors", errorHandler.getErrorDirectory());
        assertEquals(0, errorHandler.getNumParsingErrors().get());
        assertEquals(0, errorHandler.getNumDbErrors().get());
        assertEquals(0, errorHandler.getNumFileErrors().get());
        assertEquals(0, errorHandler.getNumRecordsSavedInTempDB().get());
        assertEquals("dummy header", errorHandler.getHeader());
        assertTrue(isEmpty(Path.of(errorHandler.getErrorDirectory())));
        assertEquals(2, errorHandler.getErrorThreshold());
    }

    @Test
    void getErrorFiles_noFiles() {
        StepVerifier.create(errorHandler.getErrorFiles())
                .verifyComplete();
    }


    @Test
    void getErrorFiles_TooLongParsingMessage() {
        String longMessage = getAlphaNumericString(2000);
        errorHandler.init("");
        errorHandler.handleParsingError("", "", longMessage);
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg*.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_parse*.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (0 == Files.size(p));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (0 == Files.size(p));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(1, errorHandler.getNumParsingErrors().get());
    }


    @Test
    void getErrorFiles_TooLongDBMessage() {
        String longMessage = getAlphaNumericString(2000);
        errorHandler.init("");
        errorHandler.handleDBError(new XsbData(),  longMessage);
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg*.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_db*.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (0 == Files.size(p));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (0 == Files.size(p));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(1, errorHandler.getNumDbErrors().get());
    }


    @Test
    void getErrorFiles_TooLongFileMessage() {
        String longMessage = getAlphaNumericString(2000);
        errorHandler.handleFileError("",  longMessage, new RuntimeException("Dummy"));
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg*.txt");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        return (p.toString().matches(regEx1) && (0 == Files.size(p)));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();
        assertEquals(1, errorHandler.getNumFileErrors().get());
    }


    @Test
    void handleParsingError() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String message = getAlphaNumericString(100);
        errorHandler.handleParsingError("abc~|~def", "dummyFile.gsa", message);
        message = getAlphaNumericString(100);
        errorHandler.handleParsingError("ghi~|~jkl", "dummyFile.gsa", message);
        message = getAlphaNumericString(100);
        errorHandler.handleParsingError("mno~|~pqr", "dummyFile.gsa", message);
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_0.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_parse_"+timeStamp+"_0.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(3, errorHandler.getNumParsingErrors().get());
    }


    @Test
    void handleParsingError_generateMultipleFiles() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String message = getAlphaNumericString(1900);
        errorHandler.handleParsingError("abc~|~def", "dummyFile.gsa", message);
        message = getAlphaNumericString(1900);
        errorHandler.handleParsingError("ghi~|~jkl", "dummyFile.gsa", message);
        message = getAlphaNumericString(1900);
        errorHandler.handleParsingError("mno~|~pqr", "dummyFile.gsa", message);
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_?.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_parse_"+timeStamp+"_?.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("3. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("4. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(3, errorHandler.getNumParsingErrors().get());

    }


    @Test
    void handleParsingError_generateMultipleParseFiles() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String message = getAlphaNumericString(1900);
        errorHandler.handleParsingError(message, "dummyFile.gsa", "");
        message = getAlphaNumericString(1900);
        errorHandler.handleParsingError(message, "dummyFile.gsa", "");
        message = getAlphaNumericString(1900);
        errorHandler.handleParsingError(message, "dummyFile.gsa", "");
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_?.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_parse_"+timeStamp+"_?.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("3. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("4. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("5. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("6. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(3, errorHandler.getNumParsingErrors().get());

    }



    @Test
    void handleDbError() {
        XsbData xsbData = new XsbData();
        xsbData.setSourceXsbDataString("abc~|~def");
        xsbData.setSourceXsbDataFileName("dummyFile.gsa");
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());

        String message = getAlphaNumericString(100);
        errorHandler.handleDBError(xsbData,  message);
        xsbData.setSourceXsbDataString("ghi~|~jkl");

        message = getAlphaNumericString(100);
        errorHandler.handleDBError(xsbData, message);

        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_0.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_db_"+timeStamp+"_0.gsa");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(2, errorHandler.getNumDbErrors().get());
    }


    @Test
    void handleDBError_generateMultipleFiles() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());

        XsbData xsbData = new XsbData();
        xsbData.setSourceXsbDataString("abc~|~def");
        xsbData.setSourceXsbDataFileName("dummyFile.gsa");
        String message = getAlphaNumericString(1900);
        errorHandler.handleDBError(xsbData,  message);

        xsbData.setSourceXsbDataString("ghi~|~jkl");
        message = getAlphaNumericString(1900);
        errorHandler.handleDBError(xsbData, message);

        xsbData.setSourceXsbDataString("mno~|~pqr");
        message = getAlphaNumericString(1900);
        errorHandler.handleDBError(xsbData, message);

        xsbData.setSourceXsbDataString("stu~|~vwx");
        message = getAlphaNumericString(1900);
        errorHandler.handleDBError(xsbData, message);


        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_*.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_db_"+timeStamp+"_*.gsa");


        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("3. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("4. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("5. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(4, errorHandler.getNumDbErrors().get());

    }


    @Test
    void handleMultipleErrors_generateMultipleFiles() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());

        XsbData xsbData = new XsbData();

        xsbData.setSourceXsbDataFileName("dummyFile.gsa");
        String message = getAlphaNumericString(900);
        xsbData.setSourceXsbDataString(message);
        errorHandler.handleDBError(xsbData,  message);

        xsbData.setSourceXsbDataString(getAlphaNumericString(1200));
        message = getAlphaNumericString(700);
        errorHandler.handleDBError(xsbData, message);

        message = getAlphaNumericString(1500);
        errorHandler.handleParsingError("mno~|~pqr", "dummyFile", message);

        message = getAlphaNumericString(100);
        errorHandler.handleParsingError(getAlphaNumericString(1700), "dummyFile", message);


        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_*.txt");
        String regEx2 = AcrXsbFilesUtil.globToRegex("*xsb_error_db_"+timeStamp+"_*.gsa");
        String regEx3 = AcrXsbFilesUtil.globToRegex("*xsb_error_parse_"+timeStamp+"_*.gsa");


        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2) || p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("2. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2) || p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("3. p: " + p.toString() + " regx3 " + regEx3);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2) || p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("4. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)|| p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("5. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)|| p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("6. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)|| p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .expectNextMatches(p -> {
                    try {
                        log.info("7. p: " + p.toString() + " regx2 " + regEx2);
                        return (p.toString().matches(regEx1) || p.toString().matches(regEx2)|| p.toString().matches(regEx3)) && (Files.size(p) > 0);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(2, errorHandler.getNumDbErrors().get());
        assertEquals(2, errorHandler.getNumParsingErrors().get());

    }


    @Test
    void handleFileError() {
        String timeStamp = new SimpleDateFormat("yyyyMMdd").format(new Date());
        String message = getAlphaNumericString(100);
        errorHandler.handleFileError("dummyFile1.gsa", message, new RuntimeException("dummy exception"));
        message = getAlphaNumericString(100);
        errorHandler.handleFileError("dummyFile2.gsa", message, new RuntimeException("dummy exception2"));
        message = getAlphaNumericString(100);
        errorHandler.handleFileError("dummyFile3.gsa", message, new RuntimeException("dummy exception3"));
        String regEx1 = AcrXsbFilesUtil.globToRegex("*xsb_error_msg_"+timeStamp+"_0.txt");
        errorHandler.close();
        StepVerifier.create(errorHandler.getErrorFiles())
                .expectNextMatches(p -> {
                    try {
                        log.info("1. p: " + p.toString() + " regx1 " + regEx1);
                        return (p.toString().matches(regEx1) && (Files.size(p) > 0));
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                })
                .verifyComplete();

        assertEquals(3, errorHandler.getNumFileErrors().get());
    }

    @Test
    void totalErrorsWithinAcceptableThreshold() {
        assertFalse(errorHandler.totalErrorsWithinAcceptableThreshold());
        errorHandler.setNumRecordsSavedInTempDB(new AtomicInteger(1));
        assertTrue(errorHandler.totalErrorsWithinAcceptableThreshold());
        handleFileError();
        assertTrue(errorHandler.totalErrorsWithinAcceptableThreshold());
    }

    @Test
    void totalErrorsWithinAcceptableThreshold_moreErrorsThanAccepted() {
        errorHandler.setNumRecordsSavedInTempDB(new AtomicInteger(1));
        assertTrue(errorHandler.totalErrorsWithinAcceptableThreshold());
        handleMultipleErrors_generateMultipleFiles();
        assertFalse(errorHandler.totalErrorsWithinAcceptableThreshold());
    }

    @Test
    void testBoundedPrintWWriter_messageExceedingLimit() {
        IllegalArgumentException thrown;
        try (PrintWriter printWriter = errorHandler.testBoundedPrintWriter(100)) {
            String longMessage = getAlphaNumericString(101);

            thrown = assertThrows(IllegalArgumentException.class, () -> printWriter.println(longMessage));
        }
        assertEquals("Error message is too long (101 bytes) and exceeds the maximum allowed size for the error file (100 bytes)", thrown.getMessage());
    }

    @Test
    void testBoundedPrintWWriter_messageExceedingRemainingFileSize() {
        IllegalArgumentException thrown;
        try (PrintWriter printWriter = errorHandler.testBoundedPrintWriter(100)) {
            String message = getAlphaNumericString(50);

            assertDoesNotThrow(() -> printWriter.println(message));

            String longMessage = getAlphaNumericString(51);
            thrown = assertThrows(IllegalArgumentException.class, () -> printWriter.println(longMessage));
        }
        assertEquals("File size exceeded: 105 > 100", thrown.getMessage());
    }
}