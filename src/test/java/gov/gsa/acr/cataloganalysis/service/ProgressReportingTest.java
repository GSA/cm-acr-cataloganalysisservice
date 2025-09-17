package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceFactory;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceLocal;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceS3;
import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.configuration.S3ClientConfiguration;
import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.util.EmailUtil;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest
@ActiveProfiles("junit")
@Slf4j
@MockBeans({@MockBean(XsbDataParser.class), @MockBean(XsbDataRepository.class), @MockBean(AnalysisSourceXsb.class), @MockBean(AnalysisSourceS3.class), @MockBean(TransactionalDataService.class), @MockBean(EmailUtil.class), @MockBean(JavaMailSender.class) })
@ContextConfiguration(classes = {S3ClientConfiguration.class,  AnalysisDataProcessingService.class, AnalysisSourceLocal.class, AnalysisSourceFactory.class, ErrorHandler.class})
class ProgressReportingTest {
    @Autowired
    private AnalysisDataProcessingService analysisDataProcessingService;

    @Autowired
    private XsbDataParser xsbDataParser;
    @Autowired
    private ErrorHandler errorHandler;

    Set<String> nonTAACountryCodes = Set.of("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW");

    @BeforeEach
    void setUp(){
        String validHeader = "contractNumber~|~modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";
        errorHandler.init(validHeader);
    }

    @Test
    void testProgressReporting() {
        assertEquals(1, analysisDataProcessingService.getProgressReportingIntervalSeconds());

        Path[] filesToParse = {
                Path.of("junitTestData/testFileWithErrors.gsa"),
                Path.of("junitTestData/testValidFile.gsa")
        };

        when(xsbDataParser.validateHeader(anyString())).thenReturn(true);
        when(xsbDataParser.parseXsbData(anyString(), anyString(), any(), any())).thenAnswer((Answer<XsbData>) invocationOnMock -> {
            Thread.sleep(200);
            return new XsbData();
        });

        StepVerifier.create(analysisDataProcessingService.parseXsbFiles(Flux.fromIterable(Arrays.asList(filesToParse)), nonTAACountryCodes, false, null))
                .expectNextCount(31)
                .verifyComplete();
           }
}