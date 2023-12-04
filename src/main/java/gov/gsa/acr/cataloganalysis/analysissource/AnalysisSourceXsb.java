package gov.gsa.acr.cataloganalysis.analysissource;

import com.jcraft.jsch.*;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

@Component
@Slf4j
public class AnalysisSourceXsb implements AnalysisSource {

    private final ErrorHandler errorHandler;

    @Value("${xsb.sftp.host}")
    private String host;

    @Value("${xsb.sftp.port}")
    private Integer port;

    @Value("${xsb.sftp.username}")
    private String username;

    @Value("${xsb.sftp.password}")
    private String password;

    @Value("${xsb.sftp.gsa.file.report.dir}")
    private String defaultSftpGsaFileReportDir;

    @Value("${xsb.sftp.gsa.file.upload.dir}")
    private String sftpCatalogUploadDir;

    @Value("${progress.reporting.interval.seconds:30}")
    private int progressReportingIntervalSeconds;

    public AnalysisSourceXsb(ErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    ChannelSftp createDownloadChannelSftp(String sftpGsaFilesReportDir) throws JSchException, SftpException {
        return getChannelSftp(sftpGsaFilesReportDir);
    }

    private ChannelSftp getChannelSftp(String sftpGsaFileReportDir) throws JSchException, SftpException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        long lStartTime = new Date().getTime();
        log.debug("Connecting to sftp...");
        session.connect();
        long lEndTime = new Date().getTime();
        log.debug("Connected to SFTP in : " + (lEndTime - lStartTime));
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp channelSftp = (ChannelSftp) channel;
        channelSftp.cd(sftpGsaFileReportDir);
        return (ChannelSftp) channel;
    }

    private void disconnectChannelSftp(ChannelSftp channelSftp) {
        try {
            if (channelSftp == null) return;
            if (channelSftp.isConnected()) channelSftp.disconnect();
            if (channelSftp.getSession() != null) channelSftp.getSession().disconnect();
        } catch (Exception ex) {
            log.error("SFTP disconnect error", ex);
        }
    }

    SftpProgressMonitor getSftpProgressMonitor() {
        return new SftpProgressMonitor() {
            private long totalBytesDownloadedUntilNow;
            private long totalDownloadFileSize;
            private String srcFileName, dstFileName;
            Instant start, end, lastProgressReportTime;
            private Duration progressMonitorInterval;

            @Override
            public void init(int i, String s, String s1, long l) {
                final String MN = "sftpProgressMonitor: ";
                totalDownloadFileSize = l;
                totalBytesDownloadedUntilNow = 0;
                srcFileName = s;
                dstFileName = s1;
                start = Instant.now();
                lastProgressReportTime = start;

                // Provide a progress report every so many minutes
                progressMonitorInterval = Duration.ofSeconds(progressReportingIntervalSeconds);
                log.info(MN + "Starting to download {} file {} ({} Bytes in size) to {}, reporting progress approximately every {} seconds", i, s, l, s1, progressMonitorInterval.getSeconds());
            }

            @Override
            public boolean count(long l) {
                final String MN = "sftpProgressMonitor: ";
                totalBytesDownloadedUntilNow += l;
                int percentage = (int) (totalBytesDownloadedUntilNow * 100.0 / totalDownloadFileSize + 0.5);
                Instant currentTime = Instant.now();
                if (Duration.between(lastProgressReportTime, currentTime).compareTo(progressMonitorInterval) >= 0) {
                    log.info(MN + "Downloaded {}% ({} of {} Bytes) of file: {}", percentage, totalBytesDownloadedUntilNow, totalDownloadFileSize, srcFileName);
                    lastProgressReportTime = currentTime;
                }
                return true;
            }

            @Override
            public void end() {
                end = Instant.now();
                final String MN = "sftpProgressMonitor: ";
                log.info(MN + "Finished successfully downloading {} file to {}. Time taken: {}", srcFileName, dstFileName, Duration.between(start, end));
            }
        };
    }


    Mono<Path> downloadFromXSBToLocal(String sftpGsaFilesReportDir, ChannelSftp.LsEntry entry, String destinationFolder, ChannelSftp defaultChannelSftp) {
        final String MN = "downloadFromXSBToLocal: ";
        Sinks.One<Path> sinks = Sinks.one();
        Mono<Path> downloadedPath = sinks.asMono();

        return downloadedPath.
                doOnSubscribe(subscription -> {
                    SftpProgressMonitor sftpProgressMonitor = getSftpProgressMonitor();
                    String sourceFileName = entry.getFilename();
                    String destFileName = destinationFolder + File.separator + sourceFileName;
                    ChannelSftp channelSftp = defaultChannelSftp;
                    Path dest = Path.of(destFileName);
                    try {
                        if (channelSftp == null) channelSftp = createDownloadChannelSftp(sftpGsaFilesReportDir);
                        channelSftp.get(sourceFileName, destFileName, sftpProgressMonitor);
                        sinks.tryEmitValue(dest);
                    } catch (Exception exception) {
                        log.error(MN + "Download to Local file system from SFTP FAILED. XSB file: " + sourceFileName + " Local File: " + destFileName + " " + exception.getMessage(), exception);
                        errorHandler.handleFileError(sourceFileName, "Download to Local file system from SFTP FAILED. " + exception.getMessage(), exception);
                        try {
                            Files.deleteIfExists(dest);
                        } catch (Exception e) {
                            log.error("Error deleting download file " + destFileName, e);
                        }
                        sinks.tryEmitEmpty();
                    } finally {
                        log.debug(MN + "Disconnecting from SFTP for " + entry.getFilename());
                        disconnectChannelSftp(channelSftp);
                    }
                });
    }


    /**
     * Download files from the XSB server and generate a stream of paths of the downloaded files. Since file name
     * could be a glob line pattern, there could be multiple files that may match the pattern
     *
     * @param sourceFolder      An optional source folder to search the files in SFTP server. If this is not provided
     *                          then the default "/reports" folder is searched on the server
     * @param fileNamePattern   File name for files to search. Could have wildcards (*), in which case all the
     *                          matching files will be downloaded
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of downloaded XSB files
     */
    private Flux<Path> getXSBFiles(String sourceFolder, String fileNamePattern, String destinationFolder) {
        final String MN = "getXSBFiles: ";
        ChannelSftp channelSftp = null;
        Flux<Path> rtrn = Flux.empty();
        try {
            channelSftp = createDownloadChannelSftp(sourceFolder);
            Vector<ChannelSftp.LsEntry> lsEntries = (Vector<ChannelSftp.LsEntry>) channelSftp.ls(fileNamePattern);
            rtrn = Flux.fromIterable(lsEntries)
                    .filter(lsEntry -> lsEntry.getAttrs().isReg()) // Ignore directories, block files etc. Only download regular files.
                    .publishOn(Schedulers.parallel())
                    .flatMap(entry -> downloadFromXSBToLocal(sourceFolder, entry, destinationFolder, null));
        }
        catch (Exception e) {
            log.error(MN + "SFTP failed. Error downloading file: " + fileNamePattern + ". " + e.getMessage(), e);
            errorHandler.handleFileError(fileNamePattern, "SFTP failed. " + e.getMessage(), e);
        }
        finally {
            log.debug(MN + "Disconnecting from SFTP for " + fileNamePattern);
            disconnectChannelSftp(channelSftp);
        }
        return rtrn;
    }

    /**
     * Download files from the XSB server and generate a stream of paths of the downloaded files. If multiple patterns
     * are provided in the fileNames, then each pattern might match multiple files. All these files are collected
     * on the same stream for further processing (parsing, JSON conversion, storing in DB)
     *
     * @param sourceFolder      An optional source folder to search the files in SFTP server. If this is not provided
     *                          then the default "/reports" folder is searched on the server
     * @param fileNamePatterns         An array of file names to be downloaded from the XSB server. Could be file name
     *                          patterns, in which case each pattern might return a list of files
     * @param destinationFolder Destination folder name where to save the files downloaded from the XSB server. Usually
     *                          a temporary directory that is deleted once processing completes.
     * @return A stream of all XSB files downloaded for all the patterns/ file names provided as the fileNames arg. Each
     * element in the fileNames arg could be a pattern, in which case, the stream collects all the downloaded files into
     * a single stream
     */
    public Flux<Path> getXSBFiles(String sourceFolder, Set<String> fileNamePatterns, String destinationFolder) {
        final String srcDir = (sourceFolder != null && !sourceFolder.isBlank()) ? sourceFolder : defaultSftpGsaFileReportDir;
        if (unexpectedFileNames(fileNamePatterns, log)) return Flux.empty();
        return Flux.fromIterable(fileNamePatterns).flatMap(f -> this.getXSBFiles(srcDir, f, destinationFolder));
    }

}
