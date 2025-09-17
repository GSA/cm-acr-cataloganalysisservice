package gov.gsa.acr.cataloganalysis.scheduler;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSource;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.model.Stats;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbPpApiService;
import gov.gsa.acr.cataloganalysis.util.EmailUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.*;

@Slf4j
@SpringBootTest
@ActiveProfiles("test")
@MockBeans({@MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(XsbPpApiService.class), @MockBean(AnalysisDataProcessingService.class), @MockBean(JavaMailSender.class)})
@ContextConfiguration(classes = {ScheduledTasks.class, EmailUtil.class})
class ScheduledTasksTest {

    @Autowired
    private ScheduledTasks scheduledTasks;
    @Autowired
    private XsbDataRepository xsbDataRepository;
    @Autowired
    private AnalysisSourceXsb analysisSourceXsb;
    @Autowired
    private XsbPpApiService xsbPpApiService;
    @Autowired
    private AnalysisDataProcessingService bimonthlyLoadService;

    @Test
    void testCheckAndProcessNewBimonthlyReports_InvalidACRFeedDate() {
        when(xsbPpApiService.getLatestXsbStats(Mockito.anyString())).thenReturn(Flux.empty());
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);

        // null ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.empty());
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());

        // Error while getting ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.error(new RuntimeException("testCheckAndProcessNewBimonthlyReports_InvalidACRFeedDate: Error getting ACR Date")));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());

        // Empty ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just(""));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());

        // Empty ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("invalid date format"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());

        // Empty ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2024-00-01"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());

        // IMPORTANT: This has to be the last case, otherwise it throws exceptions for anything after this
        // Error while getting ACR Feed Date
        when(xsbDataRepository.getAcrFeedDate()).thenThrow(new RuntimeException("testCheckAndProcessNewBimonthlyReports_InvalidACRFeedDate: Exception Thrown while getting ACR Date"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(xsbPpApiService, Mockito.never()).getGsaFeedDate(Mockito.anyString());
    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_InvalidGsaFeedDate() {
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-05-01"));
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);

        // Null GSA Feed Date
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.empty());
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

        // Error GSA Feed Date
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.error(new RuntimeException("testCheckAndProcessNewBimonthlyReports_InvalidGSAFeedDate: Error getting GSA Date")));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just(""));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("invalid"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("2024-13-01"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

        // IMPORTANT: This has to be the last case, otherwise it throws exceptions for anything after this
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenThrow(new RuntimeException("testCheckAndProcessNewBimonthlyReports_InvalidGSAFeedDate: Exception Thrown while getting GSA Date"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);

    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_ValidDatesGSAFeedAfterACRFeed() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-05-01"));
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("2025-06-02"));
        when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.times(1)).getBimonthlyReportNames(null);
    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_ValidDatesACRFeedAfterGSAFeed() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-05-01"));
        when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("2025-04-02"));
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.never()).getBimonthlyReportNames(null);
    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_QualifyingReportsNull() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-05-01"));
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("2025-06-02"));
        when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(null);
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.times(1)).getBimonthlyReportNames(null);
    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_QualifyingReportsEmpty() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-05-01"));
        when(xsbPpApiService.getGsaFeedDate(Mockito.anyString())).thenReturn(Mono.just("2025-09-02"));
        when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
        assertDoesNotThrow(() -> scheduledTasks.checkAndProcessNewBimonthlyReports());
        verify(analysisSourceXsb, Mockito.times(1)).getBimonthlyReportNames(null);
    }


    @Test
    void testCheckAndProcessNewBimonthlyReports_ExecutesSuccessfully() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-07-02"));
        // Given: A ScheduledTasks instance

        // When: The scheduled method is called multiple times
        // Then: It should execute without errors
        assertDoesNotThrow(() -> {
            scheduledTasks.checkAndProcessNewBimonthlyReports();
            scheduledTasks.checkAndProcessNewBimonthlyReports();
            scheduledTasks.checkAndProcessNewBimonthlyReports();
        });
    }

    @Test
    void testCheckAndProcessNewBimonthlyReports_BimonthlyAlreadyExecuting() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(true);
        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just("2025-07-02"));
        // Given: A ScheduledTasks instance

        // When: The scheduled method is called multiple times
        // Then: It should execute without errors
        assertDoesNotThrow(() -> {
            scheduledTasks.checkAndProcessNewBimonthlyReports();
        });
        verify(xsbDataRepository, Mockito.never()).getAcrFeedDate();
    }


    @Test
    void testCheckAndProcessNewBimonthlyReports_AcrFeedDateNewerThanGSAFeedDate() {
        when(bimonthlyLoadService.isExecuting()).thenReturn(false);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, 1);
        Date futureDate = cal.getTime();
        Instant instant = futureDate.toInstant();
        LocalDate localDate = instant.atZone(ZoneId.systemDefault()).toLocalDate();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        String formattedDate = localDate.format(formatter);

        when(xsbDataRepository.getAcrFeedDate()).thenReturn(Mono.just(formattedDate));
        // Given: A ScheduledTasks instance

        // When: The scheduled method is called multiple times
        // Then: It should execute without errors
        assertDoesNotThrow(() -> {
            scheduledTasks.checkAndProcessNewBimonthlyReports();
            scheduledTasks.checkAndProcessNewBimonthlyReports();
            scheduledTasks.checkAndProcessNewBimonthlyReports();
        });
    }

    @Test
    @DisplayName("Test isGsaFeedDateLaterThanAcrFeedDate with GSA feed date later than ACR feed date")
    void testIsGsaFeedDateLaterThanAcrFeedDate_GsaLater() {
        try {
            java.lang.reflect.Method compareMethod = ScheduledTasks.class.getDeclaredMethod("isGsaFeedDateLaterThanAcrFeedDate", String.class, String.class);
            compareMethod.setAccessible(true);

            boolean result = (Boolean) compareMethod.invoke(scheduledTasks, "2025-01-01", "2025-02-01");
            assertTrue(result, "GSA feed date (2025-02-01) should be later than ACR feed date (2025-01-01)");
        } catch (Exception e) {
            fail("Failed to test isGsaFeedDateLaterThanAcrFeedDate: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test isGsaFeedDateLaterThanAcrFeedDate with GSA feed date earlier than ACR feed date")
    void testIsGsaFeedDateLaterThanAcrFeedDate_GsaEarlier() {
        try {
            java.lang.reflect.Method compareMethod = ScheduledTasks.class.getDeclaredMethod("isGsaFeedDateLaterThanAcrFeedDate", String.class, String.class);
            compareMethod.setAccessible(true);

            boolean result = (Boolean) compareMethod.invoke(scheduledTasks, "2025-02-01", "2025-01-01");
            assertFalse(result, "GSA feed date (2025-01-01) should not be later than ACR feed date (2025-02-01)");
        } catch (Exception e) {
            fail("Failed to test isGsaFeedDateLaterThanAcrFeedDate: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test isGsaFeedDateLaterThanAcrFeedDate with same dates")
    void testIsGsaFeedDateLaterThanAcrFeedDate_SameDates() {
        try {
            java.lang.reflect.Method compareMethod = ScheduledTasks.class.getDeclaredMethod("isGsaFeedDateLaterThanAcrFeedDate", String.class, String.class);
            compareMethod.setAccessible(true);

            boolean result = (Boolean) compareMethod.invoke(scheduledTasks, "2025-01-01", "2025-01-01");
            assertFalse(result, "GSA feed date should not be later than ACR feed date when they are the same date");
        } catch (Exception e) {
            fail("Failed to test isGsaFeedDateLaterThanAcrFeedDate: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test isGsaFeedDateLaterThanAcrFeedDate with invalid date format")
    void testIsGsaFeedDateLaterThanAcrFeedDate_InvalidFormat() {
        try {
            java.lang.reflect.Method compareMethod = ScheduledTasks.class.getDeclaredMethod("isGsaFeedDateLaterThanAcrFeedDate", String.class, String.class);
            compareMethod.setAccessible(true);

            assertThrows(IllegalArgumentException.class, () -> {
                try {
                    compareMethod.invoke(scheduledTasks, "2025-13-01", "2025-02-01");
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException)e.getCause();
                        assertEquals(cause.getMessage(), "Invalid date format. Expected format: yyyy-MM-dd");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for invalid date format");
        } catch (Exception e) {
            fail("Failed to test isGsaFeedDateLaterThanAcrFeedDate: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test isGsaFeedDateLaterThanAcrFeedDate with null dates")
    void testIsGsaFeedDateLaterThanAcrFeedDate_NullDates() {
        try {
            java.lang.reflect.Method compareMethod = ScheduledTasks.class.getDeclaredMethod("isGsaFeedDateLaterThanAcrFeedDate", String.class, String.class);
            compareMethod.setAccessible(true);

            assertThrows(IllegalArgumentException.class, () -> {
                try {
                    compareMethod.invoke(scheduledTasks, null, "2025-02-01");
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException)e.getCause();
                        assertEquals(cause.getMessage(), "Date parameters cannot be null. Expected format: yyyy-MM-dd");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null ACR feed date");

            assertThrows(IllegalArgumentException.class, () -> {
                try {
                    compareMethod.invoke(scheduledTasks, "2025-01-01", null);
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException)e.getCause();
                        assertEquals(cause.getMessage(), "Date parameters cannot be null. Expected format: yyyy-MM-dd");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null GSA feed date");
        } catch (Exception e) {
            fail("Failed to test isGsaFeedDateLaterThanAcrFeedDate: " + e.getMessage());
        }
    }


    @Test
    @DisplayName("Test getNewSftpReportsName with date before all files - should return August 2025 files")
    void testGetNewSftpReports_Name_DateBeforeAllFiles() {
        // Use reflection to test private method
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());

            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-02-01");

            assertNotNull(result);
            assertEquals(20, result.size(), "Should return 20 files from August 2025");

            // Verify all returned files are from August 1st, 2025
            for (String filename : result) {
                assertTrue(filename.contains("20250801"),
                        "All returned files should be from August 1st, 2025: " + filename);
            }

            // Verify no files from June 2025 are included
            for (String filename : result) {
                assertFalse(filename.contains("20250625"),
                        "No files from June 2025 should be included: " + filename);
            }

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with date between June and August 2025 - should return August 2025 files")
    void testGetNewSftpReports_Name_DateBetweenJuneAndAugust() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());

            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-06-26");

            assertNotNull(result);
            assertEquals(20, result.size(), "Should return 20 files from August 2025");

            // Verify all returned files are from August 1st, 2025
            for (String filename : result) {
                assertTrue(filename.contains("20250801"),
                        "All returned files should be from August 1st, 2025: " + filename);
            }

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with date after all files - should return empty list")
    void testGetNewSftpReports_Name_DateAfterAllFiles() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());

            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-08-02");

            assertNotNull(result);
            assertEquals(0, result.size(), "Should return empty list when date is after all files");

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with exact date of latest files - should return files with that date")
    void testGetNewSftpReports_Name_ExactDateOfLatestFiles() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-08-01");

            assertNotNull(result);
            assertEquals(20, result.size(), "Should return 20 files when date equals the latest file date");

            // Verify all returned files are from August 1st, 2025
            for (String filename : result) {
                assertTrue(filename.contains("20250801"),
                        "All returned files should be from August 1st, 2025: " + filename);
            }

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with date from previous year - should return August 2025 files")
    void testGetNewSftpReports_Name_DateFromPreviousYear() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2024-12-31");

            assertNotNull(result);
            assertEquals(20, result.size(), "Should return 20 files from August 2025");

            // Verify all returned files are from August 1st, 2025
            for (String filename : result) {
                assertTrue(filename.contains("20250801"),
                        "All returned files should be from August 1st, 2025: " + filename);
            }

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with date format validation")
    void testGetNewSftpReports_Name_DateFormatValidation() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            // Test with valid date format
            assertDoesNotThrow(() -> {
                checkMethod.invoke(scheduledTasks, "2025-01-01");
            }, "Should not throw exception with valid date format");

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName date format: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName returns only files with latest date")
    void testGetNewSftpReports_Name_ReturnsOnlyLatestDateFiles() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-01-01");

            assertNotNull(result);
            assertTrue(result.size() > 0, "Should return some files");

            // Extract dates from returned filenames
            String firstFilename = result.get(0);
            String datePattern = "gsa_advantage_quarterly_job_(\\d{8})";
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(datePattern);
            java.util.regex.Matcher matcher = pattern.matcher(firstFilename);

            assertTrue(matcher.find(), "Should be able to extract date from filename");
            String extractedDate = matcher.group(1);

            // Verify all returned files have the same date
            for (String filename : result) {
                matcher = pattern.matcher(filename);
                assertTrue(matcher.find(), "Should be able to extract date from all filenames");
                assertEquals(extractedDate, matcher.group(1),
                        "All returned files should have the same date: " + filename);
            }

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName latest date logic: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName with edge case date")
    void testGetNewSftpReports_Name_EdgeCaseDate() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            // Test with date just before the June 2025 files
            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-06-24");

            assertNotNull(result);
            assertEquals(20, result.size(), "Should return 20 files from August 2025 (latest date)");

            // Verify we only get files from the latest date (August)
            boolean hasJuneFiles = result.stream().anyMatch(f -> f.contains("20250625"));
            boolean hasAugustFiles = result.stream().anyMatch(f -> f.contains("20250801"));

            assertFalse(hasJuneFiles, "Should not include files from June 2025 (not the latest date)");
            assertTrue(hasAugustFiles, "Should include files from August 2025 (latest date)");

        } catch (Exception e) {
            fail("Failed to test getNewSftpReportsName edge case: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName method exists and is accessible")
    void testGetNewSftpReportsNameMethodExists() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(initializeGsaFilenames());
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            assertNotNull(checkMethod, "getNewSftpReportsName method should exist");
            assertEquals(String.class, checkMethod.getParameterTypes()[0], "Method should take String parameter");
            assertEquals(List.class, checkMethod.getReturnType(), "Method should return List<String>");
        } catch (NoSuchMethodException e) {
            fail("getNewSftpReportsName method not found: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName method when there are no report files")
    void testGetNewSftpReportsNameNoReportFiles() {
        try {
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(null);
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-06-24");
            assertNull(result);

            // Empty list
            List<String> filenames = new ArrayList<>();
            when(analysisSourceXsb.getBimonthlyReportNames(null)).thenReturn(filenames);
            result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-06-24");
            assertNull(result);

        } catch (Exception e) {
            fail("getNewSftpReportsName threw an exception " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test getNewSftpReportsName method when there are no report files")
    void testGetNewSftpReportsName_Exception() {
        try {
            RuntimeException r = new RuntimeException("Dummy: testGetNewSftpReportsName_Exception(");
            doThrow(r).when(analysisSourceXsb).getBimonthlyReportNames(null);
            java.lang.reflect.Method checkMethod = ScheduledTasks.class.getDeclaredMethod("getNewSftpReportsName", String.class);
            checkMethod.setAccessible(true);

            assertThrows(RuntimeException.class, ()-> {
                        try {
                            List<String> result = (List<String>) checkMethod.invoke(scheduledTasks, "2025-06-24");
                        }
                        catch (InvocationTargetException i){
                            throw i.getCause();
                        }
                },
            "Should have thrown an exception"
            );

        } catch (Exception e) {
            fail("getNewSftpReportsName threw an exception " + e.getMessage());
        }
    }



    @Test
    @DisplayName("Test generateTriggerPayload method exists and is accessible")
    void testGenerateTriggerPayloadMethodExists() {
        try {
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("generateTriggerPayload", String.class, List.class);
            assertNotNull(generateMethod, "generateTriggerPayload method should exist");
            assertEquals(2, generateMethod.getParameterCount(), "Method should take no parameters");
            assertEquals(Trigger.class, generateMethod.getReturnType(), "Method should return String");
        } catch (NoSuchMethodException e) {
            fail("generateTriggerPayload method not found: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test generateTriggerPayload returns valid JSON structure")
    void testGenerateTriggerPayloadReturnsValidJson() {
        try {

            // Use reflection to access the private method
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("generateTriggerPayload", String.class, List.class);
            generateMethod.setAccessible(true);

            List<String> filenames = new ArrayList<>();

            // Add all the filenames from the user's list
            filenames.add("gsa_advantage_quarterly_job_20250801132422_11900656518300561534_report_1.gsa");
            filenames.add("gsa_advantage_quarterly_job_20250801132422_3131086686144655982_report_2.gsa");
            filenames.add("gsa_advantage_quarterly_job_20250801132422_17596505155975732618_report_3.gsa");

            // Test the method
            Trigger result = (Trigger) generateMethod.invoke(scheduledTasks, "2025-07-02", filenames);

            log.info("Payload: {}", result);
            assertNotNull(result, "generateTriggerPayload should return non-null result");
            assertEquals(Trigger.AnalysisSourceType.XSB, result.getSourceType());
            assertFalse(result.getPurgeOldData());
            assertEquals(LocalDate.parse("2025-07-02", DateTimeFormatter.ofPattern("yyyy-MM-dd")), result.getGsaFeedDate() );
            Set<String> uniqueFileNames = result.getUniqueFileNames();
            assertNotNull(uniqueFileNames);
            assertEquals(3, uniqueFileNames.size());
            assertEquals(3, result.getFiles().length);
            assertTrue(uniqueFileNames.contains("gsa_advantage_quarterly_job_20250801132422_11900656518300561534_report_1.gsa"));
            assertTrue(uniqueFileNames.contains("gsa_advantage_quarterly_job_20250801132422_3131086686144655982_report_2.gsa"));
            assertTrue(uniqueFileNames.contains("gsa_advantage_quarterly_job_20250801132422_17596505155975732618_report_3.gsa"));

        } catch (Exception e) {
            fail("Failed to test generateTriggerPayload: " + e.getMessage());
        }
    }

    @Test
    @DisplayName("Test generateTriggerPayload invalid GSA Feed Date")
    void testGenerateTriggerPayload_InvalidGSAFeedDate() {
        try {

            // Use reflection to access the private method
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("generateTriggerPayload", String.class, List.class);
            generateMethod.setAccessible(true);

            List<String> filenames = new ArrayList<>();

            // Add all the filenames from the user's list
            filenames.add("gsa_advantage_quarterly_job_20250801132422_11900656518300561534_report_1.gsa");
            filenames.add("gsa_advantage_quarterly_job_20250801132422_3131086686144655982_report_2.gsa");
            filenames.add("gsa_advantage_quarterly_job_20250801132422_17596505155975732618_report_3.gsa");

            // Null GsaFeedDate
            assertThrows(RuntimeException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, null, filenames);
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
                        assertEquals(cause.getMessage(), "Invalid GSA Feed Date: null. Cannot proceed further.");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null GSA feed date");

            // Empty GSA Feed Date
            assertThrows(RuntimeException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, "", filenames);
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
                        assertEquals(cause.getMessage(), "Invalid GSA Feed Date: . Cannot proceed further.");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null GSA feed date");

            // Invalid GSA Feed Date
            assertThrows(RuntimeException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, "Invalid", filenames);
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
                        assertEquals(cause.getMessage(), "Invalid GSA Feed Date: Invalid. Cannot proceed further.");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null GSA feed date");

            // Empty GSA Feed Dare
            assertThrows(RuntimeException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, "2024-12-35", filenames);
                } catch (Exception e) {
                    if (e.getCause() instanceof IllegalArgumentException) {
                        IllegalArgumentException cause = (IllegalArgumentException) e.getCause();
                        assertEquals(cause.getMessage(), "Invalid GSA Feed Date: 2024-12-35. Cannot proceed further.");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException for null GSA feed date");


        } catch (Exception e) {
            fail("Failed to test generateTriggerPayload: " + e.getMessage());
        }
    }


    @Test
    void testTriggerNewBimonthlyDataUpload() {
        try {
            Trigger trigger= new Trigger();
            Mockito.when(bimonthlyLoadService.triggerDataUpload(any())).thenReturn(Mono.empty());
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("triggerNewBimonthlyDataUpload", Trigger.class);
            generateMethod.setAccessible(true);
            assertDoesNotThrow(() -> generateMethod.invoke(scheduledTasks, trigger));

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void testTriggerNewBimonthlyDataUpload_ThrowsConcurrentModificationError() {
        try {
            Trigger trigger= new Trigger();
            Mockito.when(bimonthlyLoadService.triggerDataUpload(any())).thenThrow(new ConcurrentModificationException("testTriggerNewBimonthlyDataUpload_ThrowsConcurrentModificationError: Working"));
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("triggerNewBimonthlyDataUpload", Trigger.class);
            generateMethod.setAccessible(true);
            assertThrows(ConcurrentModificationException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, trigger);
                } catch (Exception e) {
                    if (e.getCause() instanceof RuntimeException) {
                        assertEquals(e.getCause().getMessage(), "Process already executing");
                        ConcurrentModificationException cause = (ConcurrentModificationException) e.getCause().getCause();
                        assertEquals(cause.getMessage(), "testTriggerNewBimonthlyDataUpload_ThrowsConcurrentModificationError: Working");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw ConcurrentModificationException");

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void testTriggerNewBimonthlyDataUpload_ThrowsIllegalArgumentError() {
        try {
            Trigger trigger= new Trigger();
            Mockito.when(bimonthlyLoadService.triggerDataUpload(any())).thenThrow(new IllegalArgumentException("testTriggerNewBimonthlyDataUpload_ThrowsIllegalArgumentError: illegal argument"));
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("triggerNewBimonthlyDataUpload", Trigger.class);
            generateMethod.setAccessible(true);
            assertThrows(IllegalArgumentException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, trigger);
                } catch (Exception e) {
                    if (e.getCause() instanceof RuntimeException) {
                        assertEquals(e.getCause().getMessage(), "The request is illegal.");
                        IllegalArgumentException cause = (IllegalArgumentException) e.getCause().getCause();
                        assertEquals(cause.getMessage(), "testTriggerNewBimonthlyDataUpload_ThrowsIllegalArgumentError: illegal argument");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw IllegalArgumentException");

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    void testTriggerNewBimonthlyDataUpload_ThrowsGenericException() {
        try {
            Trigger trigger= new Trigger();
            Mockito.when(bimonthlyLoadService.triggerDataUpload(any())).thenThrow(new NullPointerException("testTriggerNewBimonthlyDataUpload_ThrowsGenericException"));
            java.lang.reflect.Method generateMethod = ScheduledTasks.class.getDeclaredMethod("triggerNewBimonthlyDataUpload", Trigger.class);
            generateMethod.setAccessible(true);
            assertThrows(NullPointerException.class, () -> {
                try {
                    generateMethod.invoke(scheduledTasks, trigger);
                } catch (Exception e) {
                    if (e.getCause() instanceof RuntimeException) {
                        assertEquals(e.getCause().getMessage(), "Unexpected error");
                        NullPointerException cause = (NullPointerException) e.getCause().getCause();
                        assertEquals(cause.getMessage(), "testTriggerNewBimonthlyDataUpload_ThrowsGenericException");
                        throw cause;
                    }
                    throw new RuntimeException(e);
                }
            }, "Should throw NullPointerException");

        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testComposeEmailBody(){
        try {
            Method emailMessage = ScheduledTasks.class.getDeclaredMethod("composeEmailMessage", String.class, String.class, List.class);
            emailMessage.setAccessible(true);

            assertThrows(NullPointerException.class, () -> {
                try {
                    emailMessage.invoke(scheduledTasks, "2025-13-01", "2025-02-01", null);
                } catch (Exception e) {
                    if (e.getCause() instanceof NullPointerException) {
                        throw e.getCause();
                    }
                    throw new RuntimeException(e);
                }
            }, "Should thrown a NullPointerException for null list of files");

            List<String> dummyRports = new ArrayList<>();
            String result = (String) emailMessage.invoke(scheduledTasks, "1900-04-02", "2000-01-02", dummyRports);
            StringBuilder expected = new StringBuilder();
            expected.append("ACR's Catalog Analysis Service discovered new Bi-monthly reports on the XSB's SFTP server. It automatically initiated a load process. Please monitor the logs on DataDog for CAS for any updates or issues.");
            expected.append(System.lineSeparator()).append(System.lineSeparator());
            expected.append("Current ACR Feed Date: 1900-04-02");
            expected.append(System.lineSeparator());
            expected.append("New GSA Feed Date: 2000-01-02");
            expected.append(System.lineSeparator()).append(System.lineSeparator());;
            expected.append("List of Bimonthly reports: ").append(System.lineSeparator());
            assertEquals(expected.toString(), result);

            dummyRports.add("file1.gsa");
            dummyRports.add("file2.gsa");
            result = (String) emailMessage.invoke(scheduledTasks, "1900-04-02", "2000-01-02", dummyRports);
            expected.append("file1.gsa").append(System.lineSeparator());
            expected.append("file2.gsa");
            assertEquals(expected.toString(), result);


        }
        catch (Exception e) {
            fail("Failed to test composeEmailSubject: " + e.getMessage());
        }
    }

    @Test
    void testComposeEmailSubject(){
        try {
            java.lang.reflect.Method emailSubject = ScheduledTasks.class.getDeclaredMethod("composeEmailSubject", String.class, String.class);
            emailSubject.setAccessible(true);

            String result = (String) emailSubject.invoke(scheduledTasks, "Junit", "testing");
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the Junit-testing env.", result);

            result = (String) emailSubject.invoke(scheduledTasks, "", "testing");
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the testing env.", result);

            result = (String) emailSubject.invoke(scheduledTasks, null, "testing");
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the testing env.", result);

            result = (String) emailSubject.invoke(scheduledTasks, null, "");
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the  env.", result);

            result = (String) emailSubject.invoke(scheduledTasks, null, null);
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the null env.", result);

            result = (String) emailSubject.invoke(scheduledTasks, "Junit", "");
            assertEquals("ACR CAS: Cron job automatically triggered bimonthly data upload in the Junit- env.", result);

        } catch (Exception e) {
            fail("Failed to test composeEmailSubject: " + e.getMessage());
        }
    }



    /**
     * Initializes the list of GSA filenames.
     * This method contains the hardcoded list of filenames provided by the user.
     *
     * @return List of GSA filenames
     */
    private List<String> initializeGsaFilenames() {
        List<String> filenames = new ArrayList<>();

        // Add all the filenames from the user's list
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_01.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_02.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_03.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_04.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_05.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_06.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_07.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_08.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_09.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_10.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_13.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_14.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_15.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_18.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_19.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_20.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_23.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_11.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_12.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_16.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_17.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_21.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250625122050_report_22.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_10804245946520574571_report_4.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_11488314842739534680_report_6.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_11900656518300561534_report_1.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_13538147354732439698_report_11.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_15948277104262669111_report_18.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_16236352176652384759_report_16.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_17111554560132868429_report_13.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_17596505155975732618_report_3.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_18400762134157840231_report_10.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_2320834970112786258_report_19.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_3112235243342572207_report_5.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_3131086686144655982_report_2.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_3612399062957293478_report_8.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_3726362445599639528_report_9.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_4364072811573494566_report_17.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_5103102914652282401_report_12.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_6201010447959177683_report_7.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_7368942508726158188_report_14.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_7538039951446300136_report_15.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250801132422_7538039951446300136_report_20.gsa");
        filenames.add("gsa_advantage_quarterly_job_invalid_pattern_report_15.gsa");
        filenames.add("gsa_advantage_quarterly_job_20250731132422_7538039951446300136_report_20.gsa");
        filenames.add("47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa");

        return filenames;
    }

    Flux<Stats> getTestStats(){
        log.info("Inside: getTestStats");
        // Using Calendar to create specific dates
        Calendar cal = Calendar.getInstance();
        List<Stats> testData = new ArrayList<>();

        // Date 1: Current date minus 30 days
        cal.add(Calendar.DAY_OF_YEAR, -30);
        Date date1 = cal.getTime();  // e.g., If today is 2024-03-15, this will be 2024-02-14
        Stats stats1 = new Stats();
        stats1.setGsaFeedDate(date1);
        testData.add(stats1);

        // Date 2: Current date plus 45 days
        cal.setTime(new Date());  // Reset to current date
        cal.add(Calendar.DAY_OF_YEAR, 45);
        Date date2 = cal.getTime();  // e.g., If today is 2024-03-15, this will be 2024-04-29
        Stats stats2 = new Stats();
        stats1.setGsaFeedDate(date2);
        testData.add(stats2);

        // Date 3: First day of current year
        cal.setTime(new Date());
        cal.set(Calendar.DAY_OF_YEAR, 1);
        Date date3 = cal.getTime();  // e.g., 2024-01-01
        Stats stats3 = new Stats();
        stats1.setGsaFeedDate(date3);
        testData.add(stats3);


        // Date 4: Last day of previous year
        cal.setTime(new Date());
        cal.add(Calendar.YEAR, -1);
        cal.set(Calendar.MONTH, 11);  // December (0-based)
        cal.set(Calendar.DAY_OF_MONTH, 31);
        Date date4 = cal.getTime();  // e.g., 2023-12-31
        Stats stats4 = new Stats();
        stats1.setGsaFeedDate(date4);
        testData.add(stats4);

        return Flux.fromIterable(testData);
    }

}