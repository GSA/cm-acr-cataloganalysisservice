package gov.gsa.acr.cataloganalysis.util;

import com.jcraft.jsch.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Component
@Slf4j
public class AcrXsbSftpUtil {

    @Value("${xsb.sftp.host}")
    public String host;

    @Value("${xsb.sftp.port}")
    public Integer port;

    @Value("${xsb.sftp.username}")
    public String username;

    @Value("${xsb.sftp.password}")
    public String password;

    @Value("${xsb.sftp.gsa.file.report.dir}")
    public String sftpGsaFileReportDir;

    @Value("${xsb.sftp.gsa.file.upload.dir}")
    public String sftpCatalogUploadDir;

    @Value("${sftp.progress.monitor.duration.seconds:30}")
    private int progressMonitorSeconds;

    private ChannelSftp createDownloadChannelSftp() throws JSchException, SftpException {
        return getChannelSftp(sftpGsaFileReportDir);
    }

    private ChannelSftp getChannelSftp(String sftpGsaFileReportDir) throws JSchException, SftpException {
        JSch jsch = new JSch();
        Session session = jsch.getSession(username, host, port);
        session.setPassword(password);
        Properties config = new Properties();
        config.put("StrictHostKeyChecking", "no");
        session.setConfig(config);
        long lStartTime = new Date().getTime();
        log.info("Connecting to sftp...");
        session.connect();
        long lEndTime = new Date().getTime();
        log.info("Connected to SFTP in : " + (lEndTime - lStartTime));
        Channel channel = session.openChannel("sftp");
        channel.connect();
        ChannelSftp channelSftp = (ChannelSftp) channel;
        channelSftp.cd(sftpGsaFileReportDir);
        return (ChannelSftp) channel;
    }

    private ChannelSftp createUploadChannelSftp() throws Exception {
        return getChannelSftp(sftpCatalogUploadDir);

    }

    private void disconnectChannelSftp(ChannelSftp channelSftp) {
        try {
            if (channelSftp == null)
                return;

            if (channelSftp.isConnected())
                channelSftp.disconnect();

            if (channelSftp.getSession() != null)
                channelSftp.getSession().disconnect();

        } catch (Exception ex) {
            log.error("SFTP disconnect error", ex);
        }
    }

    public void deleteFiles (List<String> files){
        if (files == null) return;
        for (String file : files)
            try {
                boolean fileDeleted = Files.deleteIfExists(Paths.get(file));
                if (!fileDeleted) log.warn("Unable to delete downloaded XSB report file {}", file);
            } catch (IOException e) {
                log.error("Error deleting download file " + file, e);
            }
    }


    private Mono<Path> downloadFromXSBToLocal(ChannelSftp.LsEntry entry, String destinationFolder){
        final String MN = "downloadFromXSBToLocal: ";
        Sinks.One<Path> sinks = Sinks.one();
        Mono<Path> downloadedPath = sinks.asMono();
        return downloadedPath.
                doOnSubscribe(subscription -> {
                    SftpProgressMonitor sftpProgressMonitor =  getSftpProgressMonitor();
                    String destFileName = null;
                    String sourceFileName = entry.getFilename();
                    ChannelSftp channelSftp = null;
                    try {
                        channelSftp =  createDownloadChannelSftp();
                        SftpATTRS entryAttrs =  entry.getAttrs();
                        log.info(MN + "Downloading file {} of size {} from XSB", entry.getFilename(), entryAttrs.getSize());
                        destFileName = new StringBuilder(destinationFolder)
                                .append(File.separator).append(entry.getFilename()).toString();
                        channelSftp.get(sourceFileName, destFileName, sftpProgressMonitor);
                        sinks.tryEmitValue(Path.of(destFileName));
                        log.info(MN + "Downloaded file {} of size {} from XSB to {}", entry.getFilename(), entryAttrs.getSize(), destFileName);
                    } catch (Exception sftpException) {
                        log.error("SFTP failed downloading "+entry.getFilename() + " to " + destFileName, sftpException);

                        try {
                            Files.deleteIfExists(Paths.get(destFileName));
                        } catch (IOException e) {
                            log.error("Error deleting download file " + destFileName, e);
                        }

                        sinks.tryEmitError(new RuntimeException(MN + "Download to Local file system from SFTP FAILED. XSB file: " + sourceFileName + " Local File: " + destFileName + " " + sftpException.getMessage(), sftpException));
                    }
                    finally {
                        if (channelSftp != null) {
                            log.info(MN + "Disconnecting from SFTP for " + entry.getFilename());
                            disconnectChannelSftp(channelSftp);
                        }
                    }
                });
    }


    public Flux<Path> downloadFilesFromXSBToLocal(String fileNamePattern, String destinationFolder){
        final String MN = "downloadFilesFromXSBToLocal: ";

        ChannelSftp channelSftp = null;
        try {
            channelSftp = createDownloadChannelSftp();
            Vector<ChannelSftp.LsEntry> lsEntries = channelSftp.ls(new StringBuilder(fileNamePattern).toString());
            return Flux.fromIterable(lsEntries)
                    .publishOn(Schedulers.parallel())
                    .flatMap(entry -> downloadFromXSBToLocal(entry, destinationFolder))
                    .onErrorContinue((e,p) -> {
                        //TBD Error Handler for error
                        log.error("Could not get this file, will ignore " + p, e);
                    });
        } catch (Exception e) {
            log.error("SFTP failed", e);
            return Flux.error(new RuntimeException(MN + "Error downloading file: " + fileNamePattern + ". " + e.getMessage(), e));
        }
        finally {
            if (channelSftp != null) {
                log.info(MN + "Disconnecting from SFTP for " + fileNamePattern);
                disconnectChannelSftp(channelSftp);
            }
        }

    }

    public Flux<Path> downloadFilesFromXSBToLocal(String[] fileNames, String destinationFolder){
        final String MN = "downloadFilesFromXSBToLocal: ";
        if (fileNames == null) {
            Exception e = new IllegalArgumentException("The array must have valid file names, not NULL>");
            log.error("Error downloading files from XSB. Null argument provided. ", e);
            return Flux.error(e);
        }
        if (fileNames.length == 0 || fileNames.length > 20) {
            Exception e = new IllegalArgumentException("Either too many files to download or no files provided for download. Maximum 20 files are allowed at a time.");
            log.error("Error downloading files from XSB.", e);
            return Flux.error(e);
        }
        // Remove duplicates, if any.
        HashSet<String> uniqueItems = new HashSet<>();
        for (String fn: fileNames) uniqueItems.add(fn);

        return Flux.fromIterable(uniqueItems)
                .flatMap(f -> downloadFilesFromXSBToLocal(f, destinationFolder))
                .onErrorContinue((e, f) -> {
                    // TBD Error Handling
                    log.error ("Error downloading file " + f, e);
                });
    }

    private SftpProgressMonitor getSftpProgressMonitor() {
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
                progressMonitorInterval = Duration.ofSeconds(progressMonitorSeconds);

                log.info(MN + "Starting to download {} file {} ({} Bytes in size) to {}, reporting progress approximately every {} seconds", i, s, l, s1, progressMonitorInterval.getSeconds());
            }

            @Override
            public boolean count(long l) {
                final String MN = "sftpProgressMonitor: ";
                totalBytesDownloadedUntilNow += l;
                int percentage = (int)(totalBytesDownloadedUntilNow * 100.0 / totalDownloadFileSize + 0.5);
                Instant currentTime = Instant.now();
                if (Duration.between(lastProgressReportTime, currentTime).compareTo(progressMonitorInterval) >= 0){
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

}
