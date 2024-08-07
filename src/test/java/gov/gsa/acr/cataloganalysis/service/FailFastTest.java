package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceLocal;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import io.r2dbc.postgresql.codec.Json;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("failfasttest")
@Slf4j
@MockBeans({@MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, XsbDataParser.class, AnalysisSourceLocal.class, AnalysisSourceFactory.class, TransactionalDataService.class, ErrorHandler.class})
public class FailFastTest {
    @Value("${error.file.directory}")
    private String errorDirectory;
    @Autowired
    private ErrorHandler errorHandler;

    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    @Autowired
    private XsbDataRepository xsbDataRepository;

    @Autowired
    XsbDataParser xsbDataParser;

    @Autowired
    private AnalysisSourceS3 xsbSourceS3Files;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");


    @BeforeEach
    void setUp() throws IOException {
        String validHeader = "contractNumber~|~modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";
        errorHandler.init(validHeader);
        Files.createDirectory(Path.of("tmp"));
    }

    @AfterEach
    void tearDown() {
        analysisDataProcessingService.deleteDir(Path.of("tmp"));
    }

    @Test
    void testNumErrorsWithinThreshold() throws IOException {
        Path srcFile = Path.of("junitTestData/testFileWithInvalidHeader.gsa");
        Path vallidFile = Path.of("tmp/testFileWithInvalidHeader.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();

        srcFile = Path.of("junitTestData/testValidFile.gsa");
        vallidFile = Path.of("tmp/testValidFile.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, taaCountryCodes, true, null))
                .expectNextCount(10)
                .expectComplete()
                .verify();
    }

    @Test
    void testNumErrorsExceedThreshold() throws IOException {
        Path srcFile = Path.of("junitTestData/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa");
        Path vallidFile = Path.of("tmp/47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, taaCountryCodes, true, null))
                .expectNextCount(18)
                .expectComplete()
                .verify();


        srcFile = Path.of("junitTestData/fileWithEarlyErrors.gsa");
        vallidFile = Path.of("tmp/fileWithEarlyErrors.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, taaCountryCodes, true, null))
                .expectNextCount(7)
                .expectComplete()
                .verify();

        srcFile = Path.of("junitTestData/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa");
        vallidFile = Path.of("tmp/47QSWA18D000C-3008711_20230907134812_7055515986367968069_report_1.gsa");
        Files.copy(srcFile, vallidFile);
        assertTrue(Files.exists(vallidFile));
        StepVerifier.create(analysisDataProcessingService.parseXsbFile(vallidFile, taaCountryCodes, true, null))
                .expectNextCount(0)
                .expectComplete()
                .verify();

        assertEquals(2, errorHandler.getNumParsingErrors().get());
        assertEquals(0, errorHandler.getNumFileErrors().get());
        assertEquals(0, errorHandler.getNumDbErrors().get());
    }


    @Test
    void testFailFastForDBErrors() {
        when(xsbDataRepository.saveXSBDataToTemp(anyString(), anyString(), anyString(), any())).thenReturn(Mono.empty());

        XsbData xsbData = new XsbData();
        xsbData.setContractNumber("contract number 1");
        xsbData.setManufacturer("manufacturer 1");
        xsbData.setPartNumber("part number 1");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext()
                .expectComplete()
                .verify();

        errorHandler.handleDBError(xsbData, "Dummy error");

        xsbData = new XsbData();
        xsbData.setContractNumber("contract number 2");
        xsbData.setManufacturer("manufacturer 2");
        xsbData.setPartNumber("part number 2");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext()
                .expectComplete()
                .verify();

        xsbData = new XsbData();
        xsbData.setContractNumber("contract number 3");
        xsbData.setManufacturer("manufacturer 3");
        xsbData.setPartNumber("part number 3");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext()
                .expectComplete()
                .verify();

        assertEquals(1, errorHandler.getNumDbErrors().get());

        errorHandler.handleParsingError(xsbData.toString(), "dummy file", "dummy error");

        assertEquals(1, errorHandler.getNumParsingErrors().get());

        xsbData = new XsbData();
        xsbData.setContractNumber("contract number 4");
        xsbData.setManufacturer("manufacturer 4");
        xsbData.setPartNumber("part number 4");
        xsbData.setXsbData(Json.of("{\"dummy\": \"string\"}"));
        StepVerifier.create(analysisDataProcessingService.saveDataRecordToStaging(xsbData))
                .expectNext()
                .expectComplete()
                .verify();

    }


    @Test
    void testFailFastForParseXsbData() {
        String xsbDataString = "47QSMA21D08R6~|~123~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null);
        assertEquals("47QSMA21D08R6", xsbData.getContractNumber());
        assertEquals("AMERICAN SIGNAL COMPANY", xsbData.getManufacturer());
        assertEquals("OPT30125380", xsbData.getPartNumber());

        errorHandler.handleDBError(xsbData, "Dummy error");
        xsbData = xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null);
        assertEquals("47QSMA21D08R6", xsbData.getContractNumber());
        assertEquals("AMERICAN SIGNAL COMPANY", xsbData.getManufacturer());
        assertEquals("OPT30125380", xsbData.getPartNumber());

        xsbData = xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null);
        assertEquals("47QSMA21D08R6", xsbData.getContractNumber());
        assertEquals("AMERICAN SIGNAL COMPANY", xsbData.getManufacturer());
        assertEquals("OPT30125380", xsbData.getPartNumber());

        errorHandler.handleParsingError(xsbData.toString(), "dummy file", "dummy error");

        assertThrows(NullPointerException.class, () -> xsbDataParser.parseXsbData(xsbDataString, "testFile.gsa", taaCountryCodes, null), "ignore");
    }


    @Test
    void testTriggerDataUpload() {
        Trigger trigger = new Trigger();
        trigger.setSourceType(Trigger.AnalysisSourceType.LOCAL);
        trigger.setSourceFolder("junitTestData");
        Set<String> uniqueFileNames = new HashSet<>();
        uniqueFileNames.add("testFileWithErrors.gsa");
        trigger.setUniqueFileNames(uniqueFileNames);
        trigger.setGsaFeedDate(LocalDate.now());

        when(xsbDataRepository.saveXSBDataToTemp(anyString(),anyString(), anyString(), any())).thenReturn(Mono.empty());
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
        when(xsbDataRepository.deleteAll()).thenReturn(Mono.empty());
        when(xsbDataRepository.deleteAllXsbDataTemp()).thenReturn(Mono.empty());
        when(xsbDataRepository.findTaaCompliantCountries()).thenReturn(Flux.fromIterable(Arrays.asList("AF", "AG", "AM", "AO", "AT")));

        when(xsbSourceS3Files.uploadToS3(any(), anyString())).thenReturn(Mono.just("errors/xsb_error_parse_dummy.gsa"));

        errorHandler.setErrorDirectory(errorDirectory);

        DataUploadResults expectedResults = new DataUploadResults();
        expectedResults.setErrorFileNames(List.of("errors/xsb_error_parse_dummy.gsa", "errors/xsb_error_parse_dummy.gsa"));
        expectedResults.setNumRecordsSavedInTempDB(17);
        expectedResults.setNumFileErrors(1);
        expectedResults.setNumDbErrors(0);
        expectedResults.setNumParsingErrors(2);
        expectedResults.setForcedQuit(Boolean.FALSE);

        log.info("Triggering message: " + trigger);
        StepVerifier.create(analysisDataProcessingService.triggerDataUpload(trigger))
                .expectNext(expectedResults)
                .verifyComplete();

        Mockito.verify(xsbDataRepository, Mockito.times(17)).saveXSBDataToTemp(anyString(), anyString(), anyString(), any());
        Mockito.verify(xsbDataRepository, Mockito.times(0)).deleteAll();
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


}
