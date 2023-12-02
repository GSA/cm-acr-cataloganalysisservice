package gov.gsa.acr.cataloganalysis.util;

import com.jcraft.jsch.*;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest
@Slf4j
@MockBean(ErrorHandler.class)
@ContextConfiguration(classes ={AcrXsbSftpUtil.class,  AcrXsbFilesUtil.class})
@TestPropertySource(locations="classpath:application-junit.properties")
class AcrXsbSftpUtilTest {

    @Value("${xsb.sftp.gsa.file.report.dir}")
    private String defaultSftpGsaFileReportDir;

    @Autowired
    private AcrXsbSftpUtil acrXsbSftpUtil;

    @Autowired
    ErrorHandler errorHandler;


    @BeforeEach
    void setUp() throws IOException {
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() throws IOException {
        try (Stream<Path> stream = Files.list(Path.of("tmp"))) {
            stream.forEach(p -> {
                try {
                    Files.deleteIfExists(p);
                } catch (IOException e) {
                    log.error("Unable to delete error file: " + p, e);
                }
            });
        }
        catch (IOException e){
            log.error("Unable to delete error files.", e);
        }
        Files.deleteIfExists(Path.of("tmp"));
    }

    @Test
    void testGetXSBFiles() throws JSchException, SftpException {
        log.info("Listing files from {} directory", defaultSftpGsaFileReportDir);

        // Since the files on the XSB SFTP server might change every time this test is executed, we have to first find
        // some valid files that could be used in our test case here. THe selection criteria is --
        // Regular Files less than 50KB in size
        // If possible the file name should have a common pattern so the test case can select files using a glob wilecard
        // Find a file that satisfies the size criteria listed above and there is only one of its kind.
        ChannelSftp channelSftp = acrXsbSftpUtil.createDownloadChannelSftp(defaultSftpGsaFileReportDir);
        Vector<ChannelSftp.LsEntry> lsEntries = (Vector<ChannelSftp.LsEntry>) channelSftp.ls(defaultSftpGsaFileReportDir);
        Map<String, Integer> potentialFiles = new HashMap<>();
        for(ChannelSftp.LsEntry lsEntry: lsEntries){
            SftpATTRS attrs = lsEntry.getAttrs();
            // Get only regular files
            if (attrs.isReg()) {
                String fileName = lsEntry.getFilename();
                String fileNamePattern = fileName.substring(0, Math.min(fileName.length(), 13));
                // If we have already encountered this file pattern
                if (potentialFiles.containsKey(fileNamePattern)) {
                    // Put a high number if the size of the file size is over our limit, so it is ignored
                    if (attrs.getSize() > 50000) potentialFiles.put(fileNamePattern, 1000);
                    else {
                        int count = potentialFiles.get(fileNamePattern);
                        potentialFiles.put(fileNamePattern, count + 1);
                    }
                }
                // We are encountering this file name pattern for the first time
                else {
                    // If the file is too large, ignore it by putting a high value for it. This high number will be
                    // rejected in the next criteria.
                    if (attrs.getSize() > 50000) potentialFiles.put(fileNamePattern, 1000);
                    else potentialFiles.put(fileNamePattern, 1);

                }
            }
        }

        String pattern1 = null, pattern2 = null;
        int numFiles1 = 0, numFiles2 = 0;
        for (Map.Entry<String, Integer> entry : potentialFiles.entrySet()) {
            if (entry.getValue() > 2 && entry.getValue() < 10 && pattern1 == null) {
                pattern1 = entry.getKey(); numFiles1 = entry.getValue();
            }
            if (entry.getValue() == 1 && pattern2 == null){
                pattern2 = entry.getKey();numFiles2= entry.getValue();
            }
        }

        Set<String> filePatterns = new HashSet<>();
        if (pattern1 != null) filePatterns.add(pattern1 + "*");
        if (pattern2 != null) filePatterns.add(pattern2 + "*");

        // Add a bogus file as well. This should not change the results.
        filePatterns.add("invalidFile.gsa");

        final int expectedCount = numFiles1+numFiles2;

        String regEx1 = StringUtils.globToRegex("tmp*" + pattern1 + "*");
        String regEx2 = StringUtils.globToRegex("tmp*" + pattern2 + "*");

        StepVerifier.Step<Path> step = StepVerifier.create(acrXsbSftpUtil.getXSBFiles(null, filePatterns, "tmp"));
        for (int i = 0; i < expectedCount; i++) step = step.expectNextMatches(p -> p.toString().matches(regEx1) || p.toString().matches(regEx2));

        step.expectComplete().verify();
    }


    @Test
    void testValidSourceFolder() {
        // Test valid Source Folder
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles(null, null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("", null, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("invalidDirectory", null, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidFileNames() {
        // Test valid Filenames
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", null, null))
                .expectComplete()
                .verify();

        HashSet<String> testFileNames = new HashSet<>();
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        // Create a set with more than 20 items, to test the upper limit of 20 items only.
        Set<String> set = IntStream.rangeClosed(Character.MIN_VALUE, Character.MAX_VALUE)
                .filter(Character::isLowerCase)
                .mapToObj(i -> Character.valueOf((char) i).toString())
                .collect(Collectors.toSet());
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", set, null))
                .expectComplete()
                .verify();
    }

    @Test
    void testValidDestinationFolder() {
        // Test valid destination folder
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, null))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, ""))
                .expectComplete()
                .verify();

        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, "invalidDirectory"))
                .expectComplete()
                .verify();
    }


    @Test
    void testNoMatchingFiles() {
        HashSet<String> testFileNames = new HashSet<>();
        testFileNames.add("oneFile.gsa");
        StepVerifier.create(acrXsbSftpUtil.getXSBFiles("junitTestData", testFileNames, "tmp"))
                .expectComplete()
                .verify();
    }


    @Test
    void testProgressMonitorProgressReporting() throws InterruptedException {
        SftpProgressMonitor sftpProgressMonitor = acrXsbSftpUtil.getSftpProgressMonitor();
        sftpProgressMonitor.init(1, "file1", "file2", 100);
        sftpProgressMonitor.count(20);
        Thread.sleep(300);
        sftpProgressMonitor.count(20);
        Thread.sleep(300);
        sftpProgressMonitor.count(20);
        Thread.sleep(300);
        sftpProgressMonitor.count(10);
        Thread.sleep(300);
        sftpProgressMonitor.count(20);
        Thread.sleep(100);
        sftpProgressMonitor.count(10);
        sftpProgressMonitor.end();
    }

    @Test
    void testDownloadFromXsbToLocalWithExceptions() throws SftpException, JSchException {
        ChannelSftp channelSftp = Mockito.mock(ChannelSftp.class);
        ChannelSftp.LsEntry entry = Mockito.mock(ChannelSftp.LsEntry.class);
        Mockito.when(entry.getFilename()).thenReturn("aDummyFile");
        RuntimeException r = new RuntimeException("Dummy");
        doThrow(r).when(channelSftp).get(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.when(channelSftp.isConnected()).thenReturn(false);
        Mockito.when(channelSftp.getSession()).thenReturn(null);

        StepVerifier.create(acrXsbSftpUtil.downloadFromXSBToLocal("file1", entry, "file2", channelSftp))
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError("aDummyFile", "Download to Local file system from SFTP FAILED. Dummy", r );

    }

    @Test
    void testDownloadFromXsbToLocalWithExceptionCleanupFails() throws SftpException, JSchException, IOException {
        MockedStatic<Files> mockedSettings;
        mockedSettings = mockStatic(Files.class);
        when(Files.deleteIfExists(any())).thenThrow(new IOException("Dummy"));

        ChannelSftp channelSftp = Mockito.mock(ChannelSftp.class);
        ChannelSftp.LsEntry entry = Mockito.mock(ChannelSftp.LsEntry.class);
        Mockito.when(entry.getFilename()).thenReturn("aDummyFile");
        RuntimeException r = new RuntimeException("Dummy");
        doThrow(r).when(channelSftp).get(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.when(channelSftp.isConnected()).thenReturn(false);
        Mockito.when(channelSftp.getSession()).thenReturn(null);

        StepVerifier.create(acrXsbSftpUtil.downloadFromXSBToLocal("file1", entry, "file2", channelSftp))
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError("aDummyFile", "Download to Local file system from SFTP FAILED. Dummy", r );
        mockedSettings.close();
    }


    @Test
    void testDownloadFromXsbToLocalWithDisconnectException() throws SftpException, JSchException {
        ChannelSftp channelSftp = Mockito.mock(ChannelSftp.class);
        ChannelSftp.LsEntry entry = Mockito.mock(ChannelSftp.LsEntry.class);
        Mockito.when(entry.getFilename()).thenReturn("aDummyFile");
        RuntimeException r = new RuntimeException("Dummy");
        doThrow(r).when(channelSftp).get(Mockito.anyString(), Mockito.anyString(), Mockito.any());
        Mockito.when(channelSftp.isConnected()).thenReturn(true);
        doThrow(r).when(channelSftp).disconnect();
        Mockito.when(channelSftp.getSession()).thenReturn(null);

        StepVerifier.create(acrXsbSftpUtil.downloadFromXSBToLocal("file1", entry, "file2", channelSftp))
                .verifyComplete();

        Mockito.verify(errorHandler, Mockito.times(1)).handleFileError("aDummyFile", "Download to Local file system from SFTP FAILED. Dummy", r );

    }




}