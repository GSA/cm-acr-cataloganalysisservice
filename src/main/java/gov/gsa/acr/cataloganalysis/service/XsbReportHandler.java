package gov.gsa.acr.cataloganalysis.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.model.XsbDataJsonRecord;
import io.r2dbc.postgresql.codec.Json;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
@Slf4j
public class XsbReportHandler {
    private static String DELIM;

    @Value("${xsb.report.file.delimiter}")
    private String delimiter;
    private static String[] header;

    @PostConstruct
    public void init(){DELIM = delimiter;}
    public static void setHeader(String rawHeaderString) {
        if (rawHeaderString != null) header = rawHeaderString.split(DELIM, -1);
    }

    public static XsbDataJsonRecord mapRawXsbResponseToJson(Map<String, String>xsbResponseAsAMap){
        if (xsbResponseAsAMap == null) throw new IllegalArgumentException("A Blank XSB data row.");
        return new XsbDataJsonRecord(xsbResponseAsAMap);
    }

    public static XsbData mapRawXsbResponseToXsbDataPojo(String rawXsbResponseString){
        if (rawXsbResponseString.isBlank()) throw new IllegalArgumentException("A Blank XSB data row.");
        String [] xsbResponseAsArray = rawXsbResponseString.split(DELIM, -1);

        if (header == null || xsbResponseAsArray.length != header.length)
            throw new IllegalArgumentException("Invalid XSB data row. The number of fields do not match expected count. Expected " + header.length + ", found " + xsbResponseAsArray.length);

        Map<String, String> xsbResponseAdMap = IntStream.range(0, header.length)
                .boxed()
                .collect(Collectors.toMap(k -> header[k], v -> xsbResponseAsArray[v]));



        XsbData xsbData = new XsbData();
        xsbData.setRawXSBResponseData(rawXsbResponseString);
        xsbData.setContractNumber(xsbResponseAdMap.get("contractNumber"));
        xsbData.setManufacturer(xsbResponseAdMap.get("manufacturerName"));
        xsbData.setPartNumber(xsbResponseAdMap.get("manufacturerPartNumber"));

        try {
            ObjectMapper objectMapper = new ObjectMapper();
            XsbDataJsonRecord xsbDataJsonRecord = mapRawXsbResponseToJson(xsbResponseAdMap);
            xsbData.setXsbData(Json.of(objectMapper.writeValueAsString(xsbDataJsonRecord)));
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Could not convert XSB response data to JSON. " + e.getMessage());
        }

        return xsbData;
    }
}
