package gov.gsa.acr.cataloganalysis.scheduler;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbPpApiService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
@Component
public class ScheduledTasks {
    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    // Regex pattern to extract date from GSA filenames
    private static final Pattern GSA_FILENAME_PATTERN = Pattern.compile(
            "gsa_advantage_quarterly_job_(\\d{8})\\d{6}.*\\.gsa"
    );

    private static final Pattern VALID_DATE_PATTERN = Pattern.compile(
            "^(?:19|20)\\d\\d-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12]\\d|3[01])$"
    );

    private final XsbDataRepository xsbDataRepository;
    private final AnalysisSourceXsb xsb;
    private final XsbPpApiService xsbPpApiService;
    private final AnalysisDataProcessingService analysisDataProcessingService;


    public ScheduledTasks(XsbDataRepository xsbDataRepository, AnalysisSourceXsb xsb, XsbPpApiService xsbPpApiService, AnalysisDataProcessingService analysisDataProcessingService) {
        this.xsbDataRepository = xsbDataRepository;
        this.xsb = xsb;
        this.xsbPpApiService = xsbPpApiService;
        this.analysisDataProcessingService = analysisDataProcessingService;
    }

    /**
     * Scheduled task for checking and processing new Bimonthly SFTP reports.
     * Checks for new reports on XSB's SFTP server based on the configured schedule.
     * Schedule is configured via 'app.scheduler.bimonthly-check-cron' property.
     * Default: every 5 minutes.
     *
     * Note: To disable this task, set the cron property to "0 0 0 31 2 ?"
     * which schedules it for February 31st (which never occurs)
     */
    @Scheduled(cron = "${app.scheduler.bimonthly-check-cron}")
    public void checkAndProcessNewBimonthlyReports() {
        try {
            if (analysisDataProcessingService.isExecuting()) {
                log.warn("Bimonthly Data Upload Task could not proceed, as a previous execution to load the bimonthly data is still runnin.");
                return;
            }

            String acrFeedDate = xsbDataRepository.getAcrFeedDate().block();

            if (acrFeedDate == null || acrFeedDate.isEmpty() || !VALID_DATE_PATTERN.matcher(acrFeedDate).matches())
                throw new RuntimeException("Invalid ACR Feed Date: "+acrFeedDate+". Cannot proceed further.");

            String gsaFeedDate = xsbPpApiService.getGsaFeedDate(acrFeedDate).block();
            if (gsaFeedDate == null || gsaFeedDate.isEmpty() ){
                log.info("checkAndProcessNewBimonthlyReports: No new bimonthly reports to process yet, we will check again later.");
            }

            if (!VALID_DATE_PATTERN.matcher(gsaFeedDate).matches())
                throw new RuntimeException("Invalid GSA Feed Date: "+gsaFeedDate+". Cannot proceed further.");

            String triggerPayload;
            if (isGsaFeedDateLaterThanAcrFeedDate(acrFeedDate, gsaFeedDate)){
                List<String> qualifyingReports = getNewSftpReportsName(gsaFeedDate);
                if (qualifyingReports != null && !qualifyingReports.isEmpty()){
                    triggerPayload = generateTriggerPayload(gsaFeedDate, qualifyingReports);
                    // TBD: Trigger the process
                    log.info("Checking SFTP for new bimonthly reports at {}. AcrFeedDate: {}, GsaFeedDate: {}, newGsaFeedDate? {}, triggerPayload: {}",
                            dateTimeFormatter.format(LocalDateTime.now()), acrFeedDate, gsaFeedDate,
                            isGsaFeedDateLaterThanAcrFeedDate(acrFeedDate, gsaFeedDate), triggerPayload);
                }
                else {
                    log.info("checkAndProcessNewBimonthlyReports: No new bimonthly reports to process yet, we will check again later.");
                }
            }
            else {
                log.info("checkAndProcessNewBimonthlyReports: No new bimonthly reports to process yet, we will check again later.");
            }
        } catch (Exception e) {
            log.error("Scheduled job to check and process new bimonthly reports failed.", e);
        }
    }


    /**
     * Compares two dates and returns true if gsaFeedDate is later than acrFeedDate.
     *
     * @param acrFeedDate ACR feed date string in "yyyy-MM-dd" format
     * @param gsaFeedDate GSA feed date string in "yyyy-MM-dd" format
     * @return true if gsaFeedDate is later than acrFeedDate, false otherwise
     * @throws IllegalArgumentException if either date string is not in valid "yyyy-MM-dd" format
     */
    private boolean isGsaFeedDateLaterThanAcrFeedDate(String acrFeedDate, String gsaFeedDate) {
        // Check for null parameters
        if (acrFeedDate == null || gsaFeedDate == null) {
            throw new IllegalArgumentException("Date parameters cannot be null. Expected format: yyyy-MM-dd");
        }

        try {
            LocalDate acrDate = LocalDate.parse(acrFeedDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            LocalDate gsaDate = LocalDate.parse(gsaFeedDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            return gsaDate.isAfter(acrDate);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Invalid date format. Expected format: yyyy-MM-dd", e);
        }
    }


    /**
     * Checks for new SFTP reports based on a given date.
     * Returns a list of filenames that have dates on or after the input date,
     * but only includes files with the greatest (latest) date among qualifying files.
     *
     * @param dateString Date string in "YYYY-MM-dd" format to compare against
     * @return List of qualifying filenames with the latest date
     */
    private List<String> getNewSftpReportsName(String dateString) {
        // Initialize the list of all available filenames
        List<String> allFilenames = xsb.getBimonthlyReportNames(null);
        if (allFilenames == null || allFilenames.isEmpty()) return null;

        // Parse the input date
        LocalDate inputDate = LocalDate.parse(dateString, DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        // Extract dates from filenames and find the latest qualifying date
        LocalDate latestQualifyingDate = null;
        List<String> qualifyingFilenames = new ArrayList<>();

        for (String filename : allFilenames) {
            Matcher matcher = GSA_FILENAME_PATTERN.matcher(filename);
            if (matcher.matches()) {
                String dateStr = matcher.group(1); // Extract YYYYMMdd portion
                LocalDate fileDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));

                // Check if file date is on or after the input date
                if (fileDate.isAfter(inputDate) || fileDate.isEqual(inputDate)) {
                    // Update latest qualifying date if this date is newer
                    if (latestQualifyingDate == null || fileDate.isAfter(latestQualifyingDate)) {
                        latestQualifyingDate = fileDate;
                        // Clear previous qualifying files since we found a newer date
                        qualifyingFilenames.clear();
                        qualifyingFilenames.add(filename);
                    } else if (fileDate.isEqual(latestQualifyingDate)) {
                        // Add this file since it has the same latest date
                        qualifyingFilenames.add(filename);
                    }
                }
            }
        }

        log.info("Found {} qualifying files with date {} for input date {}",
                qualifyingFilenames.size(),
                latestQualifyingDate != null ? latestQualifyingDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) : "none",
                dateString);

        return qualifyingFilenames;
    }

    /**
     * Generates a trigger payload JSON containing source type, purge flag, GSA feed date, and file list.
     * This method combines the determineGsaFeedDate method to get the latest GSA feed date
     * and the checkNewSftpReports method to get the list of qualifying files.
     *
     * @return JSON string representing the trigger payload
     */
    private String generateTriggerPayload(String gsaFeedDate, List<String> qualifyingFiles) {
        if (gsaFeedDate == null || gsaFeedDate.isEmpty() || !VALID_DATE_PATTERN.matcher(gsaFeedDate).matches())
            throw new IllegalArgumentException("Invalid GSA Feed Date: "+gsaFeedDate+". Cannot proceed further.");

        // Build the JSON payload
        StringBuilder jsonBuilder = new StringBuilder();
        jsonBuilder.append("{\n");
        jsonBuilder.append("    \"sourceType\": \"XSB\",\n");
        jsonBuilder.append("    \"purgeOldData\": false,\n");
        jsonBuilder.append("    \"gsaFeedDate\": \"").append(gsaFeedDate).append("\",\n");
        jsonBuilder.append("    \"files\": [\n");

        // Add each file to the JSON array
        for (int i = 0; i < qualifyingFiles.size(); i++) {
            jsonBuilder.append("        \"").append(qualifyingFiles.get(i)).append("\"");
            if (i < qualifyingFiles.size() - 1) {
                jsonBuilder.append(",");
            }
            jsonBuilder.append("\n");
        }

        jsonBuilder.append("    ]\n");
        jsonBuilder.append("}");

        String result = jsonBuilder.toString();
        log.info("Generated trigger payload with {} files and GSA feed date: {}", qualifyingFiles.size(), gsaFeedDate);

        return result;
    }


}
