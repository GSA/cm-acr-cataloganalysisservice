package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbDataParser {
    private final ErrorHandler errorHandler;
    @Value("${xsb.report.file.delimiter}")
    private String defaultDelimiter;
    @Value("${xsb.report.file.header}")
    @Getter
    private String baselineHeaderString;
    @Value("${xsb.report.file.extended.header}")
    private String extendedHeaderString;
    @Getter
    private String delimRegex;
    @Getter
    private String delimString;
    @Getter
    private String[] baselineHeader;
    @Getter
    private String[] extendedHeader;

    @PostConstruct
    public void init(){
        delimRegex = defaultDelimiter;
        delimString = delimRegex.replace("\\", "");
        baselineHeader = baselineHeaderString.split(delimRegex, -1);
        extendedHeader = extendedHeaderString.split(delimRegex, -1);
    }

    public boolean validateHeader(String rawHeaderString){
        if (rawHeaderString == null) return false;
        if (rawHeaderString.startsWith(extendedHeaderString)) return true;
        else return rawHeaderString.startsWith(baselineHeaderString);
    }

    private void validateRequest(String xsbDataString){
        if (xsbDataString == null || xsbDataString.isBlank())
            throw new IllegalArgumentException("A Blank XSB data row.");
        if (!xsbDataString.contains(delimString))
            throw new IllegalArgumentException("Input string is not formatted correctly. it is not delimited with expected delimiter, " + delimString);
    }

    public String[] parseXsbDataToArray(String xsbDataString){
        // Validate the string. Throw exception if not valid. Otherwise, continue parsing.
        validateRequest(xsbDataString);
        String [] xsbDataAsArray = xsbDataString.split(delimRegex, -1);
        if (baselineHeader.length > xsbDataAsArray.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + baselineHeader.length + ", found " + xsbDataAsArray.length);
        if (baselineHeader.length < xsbDataAsArray.length && extendedHeader.length > xsbDataAsArray.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + extendedHeader.length + ", found " + xsbDataAsArray.length);

        return xsbDataAsArray;
    }

    public Map<String, String> parseXsbDataToMap(String xsbDataString){
        String [] xsbDataAsArray = parseXsbDataToArray(xsbDataString);
        int numberOfFields = Math.min(xsbDataAsArray.length, this.extendedHeader.length);
        return IntStream.range(0, numberOfFields)
                .boxed()
                .collect(Collectors.toMap(k -> this.extendedHeader[k], v -> xsbDataAsArray[v]));
    }

    public XsbData parseXsbData(String xsbDataString, String sourceFileName, Set<String> nonTAACountryCodes, LocalDate gsaFeedDate){
        // Check if we have too many errors already. If yes, no point moving forward, bail off now.
        if (!errorHandler.totalErrorsWithinAcceptableThreshold()) throw new NullPointerException("ignore");
        // Check if we are asked to force quit.
        if (errorHandler.getForceQuit()) {
            log.info("Terminating: The process is being forced to exit!");
            throw new NullPointerException("ignore");
        }

        if (sourceFileName == null || sourceFileName.isBlank())
            throw new IllegalArgumentException("A Null source file name.");
        if (nonTAACountryCodes == null || nonTAACountryCodes.isEmpty())
            throw new IllegalArgumentException("invalid list of Trade agreement country codes, either null or empty.");
        Map<String, String> parsedDataAsMap = parseXsbDataToMap(xsbDataString);
        if (gsaFeedDate != null)
            parsedDataAsMap.put("gsaFeedDate", gsaFeedDate.toString());
        XsbData xsbData = new XsbData(parsedDataAsMap, nonTAACountryCodes);
        xsbData.setSourceXsbDataString(xsbDataString);
        xsbData.setSourceXsbDataFileName(sourceFileName);
        return xsbData;
    }
}
