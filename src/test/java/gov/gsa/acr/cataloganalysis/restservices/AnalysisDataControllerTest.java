package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.error.ErrorHandler;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbDataParser;
import gov.gsa.acr.cataloganalysis.util.TokenService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.MockBeans;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ConcurrentModificationException;
import java.util.Set;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@MockBeans({@MockBean(XsbDataRepository.class), @MockBean(TokenService.class)})
@AutoConfigureWebTestClient
@Slf4j
@TestPropertySource(locations="classpath:application-test.properties")
class AnalysisDataControllerTest {
    @Autowired
    WebTestClient webTestClient;

    @Autowired
    XsbDataParser xsbDataParser;

    @Autowired
    TokenService tokenService;

    @Autowired
    XsbDataRepository xsbDataRepository;

    @Autowired
    private ErrorHandler errorHandler;

   @MockBean
    private AnalysisDataProcessingService analysisDataProcessingService;

    Set<String> nonTAACountryCodes = Set.of("AD", "AE", "AL", "AR", "AZ", "BA", "BN", "BO", "BR", "BW", "BY", "CG", "CI", "CM", "CN", "DZ", "EC", "EG", "EH", "FJ", "GE", "GH", "GP", "ID", "IN", "IQ", "JO", "KE", "KG", "KW", "KZ", "LB", "LK", "LY", "MC", "MH", "MK", "MN", "MO", "MU", "MV", "MY", "NA", "NG", "NR", "NU", "PG", "PH", "PK", "PW", "PY", "QA", "RS", "RU", "SA", "SC", "SM", "SR", "SY", "SZ", "TH", "TJ", "TL", "TM", "TN", "TO", "TR", "UY", "UZ", "VE", "VN", "ZA", "ZW");



    @BeforeEach
    void setUp() {
        when(tokenService.validate(any())).thenReturn(true);
        String validHeader = "contractNumber~|~modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";
        errorHandler.init(validHeader);
    }

    @Test
    void testTriggerEmptyBody() {
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class).value(response -> assertThat(response).contains("Bad Request"));
    }

    @Test
    void triggerEndPoint() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.empty());

        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nTriggered\n"));
    }

    @Test
    void trigger_alreadyWorking() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(new ConcurrentModificationException("Working"));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isEqualTo(503)
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nWorking\n"));
    }


    @Test
    void trigger_error() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenReturn(Mono.error(new RuntimeException("Dummy")));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class).value(response -> assertThat(response).isEqualTo("\nTriggered\n"));
    }


    @Test
    void trigger_unexpectedException() {
        Trigger trigger= new Trigger();
        Mockito.when(analysisDataProcessingService.triggerDataUpload(any())).thenThrow(new RuntimeException("Dummy"));
        webTestClient
                // Create a GET request to test an endpoint
                .post().uri("/api/trigger")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Mono.just(trigger), Trigger.class)
                .exchange()
                .expectStatus().is5xxServerError()
                .expectBody(String.class).value(response -> assertThat(response).isEqualToIgnoringNewLines("Dummy"));
    }


    @Test
    void testGetETS() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllETS()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/ets")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }

    @Test
    void testGetLowOutlier() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllLowOutliers()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/low-outlier")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }

    @Test
    void testGetMiaRiskProducts() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllMIARisk()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/mia-risk")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }

    @Test
    void testGetExceedsMarketThresholdProducts() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllExceedsMarketThreshold()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/exceeds-market-threshold")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }

    @Test
    void testGetProhibitedProducts() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllProhibitedProducts()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/isProhibited")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }

    @Test
    void testGetTaaRisk() {
        String xsbDataString1 = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~dummy~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~dummy~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~~|~";
        XsbData x1 = xsbDataParser.parseXsbData(xsbDataString1, "testFile.gsa", nonTAACountryCodes, null);
        XsbData [] dummyXsbData = {x1};
        Mockito.when(xsbDataRepository.findAllTAARisk()).thenReturn(Flux.fromArray(dummyXsbData));

        webTestClient
                // Create a GET request to test an endpoint
                .get().uri("/api/taa-risk")
                .exchange()
                .expectStatus().isOk()
                .expectBody(XsbData.class).value(response -> assertThat(response.getPartNumber()).isEqualTo(x1.getPartNumber()));
    }
}