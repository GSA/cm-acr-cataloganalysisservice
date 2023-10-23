package gov.gsa.acr.cataloganalysis.service;

import gov.gsa.acr.cataloganalysis.model.XsbData;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;


@SpringBootTest
@Slf4j
@ContextConfiguration(classes = {XsbDataParser.class})
@TestPropertySource(locations="classpath:application-test.properties")
class XsbDataParserTest {

    @Autowired
    XsbDataParser xsbDataParser;

    List<String> taaCountryCodes = Arrays.asList("AF", "AG", "AM", "AO", "AT", "AU", "AW", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BQ", "BS", "BT", "BZ", "CA", "CD", "CF", "CH", "CL", "CO", "CR", "CW", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EE", "ER", "ES", "ET", "FI", "FR", "GB", "GD", "GM", "GN", "GQ", "GR", "GS", "GT", "GW", "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IS", "IT", "JM", "JP", "KH", "KI", "KM", "KN", "KR", "LA", "LC", "LI", "LR", "LS", "LT", "LU", "LV", "MA", "MD", "ME", "MG", "ML", "MR", "MS", "MT", "MW", "MX", "MZ", "NE", "NI", "NL", "NO", "NP", "NZ", "OM", "PA", "PE", "PL", "PT", "RO", "RW", "SB", "SE", "SG", "SI", "SK", "SL", "SN", "SO", "SS", "ST", "SV", "SX", "TD", "TG", "TP", "TT", "TV", "TW", "TZ", "UA", "UG", "US", "VC", "VG", "VU", "WS", "YE", "ZM", "XX");

    private String validHeader = "contractNumber~|~modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";


    @Test
    void init() {
        assertEquals("\\~\\|\\~", xsbDataParser.getDelimRegex());
        assertEquals("~|~", xsbDataParser.getDelimString());
        assertEquals(validHeader, xsbDataParser.getHeaderString());
    }

    @Test
    void testValidateHeader() {
        assertEquals(true, xsbDataParser.validateHeader(validHeader));

        String[] expectedHeader = {"contractNumber","modificationNumber","vendorName","vendorPartNumber","vendorDescription","bpaNumber","solicitationNumber","dunsNumber","sin","sinInference","sinsMatch","manufacturerName","manufacturerPartNumber","quantityOfUnit","quantityPerUnit","unitOfIssue","standardizedManufacturerName","standardizedManufacturerPartNumber","standardizedUnitOfIssue","fsc","nsn","unspsc","globalPackagingIdentifier","standardizedGlobalPackagingIdentifier","productName","productType","standardizedProductName","standardizedProductDescription","uniqueItemIdentifier","standardizedPackageQuantity","hits","selfHits","abilityOneItem","bioPreferred","comprehensiveProcurementGuidelineCompliant","significantNewAlternativesPolicyApproved","federalEnergyManagementProgramEnergyEfficientItem","waterSense","saferChoice","energyStar","epeat","primeItem","epaPrimaryMetalsFree","lowVoc","ets","deliveryFob","uniqueItemIdentifierScore","finalPrice","lowPriceTarget","midPriceTarget","highPriceTarget","catalogMinPrice","catalogAvgPrice","catalogMedianPrice","catalogMaxPrice","catalogPriceStandardDeviation","transactionMinPrice","transactionAvgPrice","transactionMedianPrice","transactionMaxPrice","catalogMinPriceSupplier","catalogMedianPriceSupplier","catalogMaxPriceSupplier","catalogMinPriceDelta","catalogAvgPriceDelta","catalogMedianPriceDelta","commercialCatalogLowPriceTarget","commercialCatalogMidPriceTarget","commercialCatalogHighPriceTarget","commercialCatalogMinPrice","commercialCatalogAvgPrice","commercialCatalogMedianPrice","commercialCatalogMaxPrice","commercialCatalogPriceStandardDeviation","commercialCatalogMinPriceSupplier","commercialCatalogMedianPriceSupplier","commercialCatalogMaxPriceSupplier","countryOriginInference","demandWeightedIndexScore","rankCategory","salesLikelihood","catalogMinPriceSource","catalogMedianPriceSource","catalogMaxPriceSource","isAuthorizedVendor","isProhibited","prohibitionCondition","prohibitionReason","prohibitionComment","fedmallMinPrice","fedmallMedPrice","fedmallAvgPrice","fedmallMaxPrice","nasaSewpMinPrice","nasaSewpMedPrice","nasaSewpAvgPrice","nasaSewpMaxPrice","vppSupplyCategoryId","vppIndicator","itemIdentifier","systemOfRecord","annualDemandQuantity","standardizedSinPrevalence","userDefinedInput","countryOrigin","unattributedManufacturerPartNumber","isInvalid","invalidReason","tdrAvgPrice","tdrMaxPrice","tdrMedianPrice","tdrMinPrice"};
        assertArrayEquals(expectedHeader, xsbDataParser.getHeader());
    }

    @Test
    void testNullHeader() {
        boolean b = xsbDataParser.validateHeader(null);
        assertEquals(false, b);
    }

    @Test
    void testBlankHeader() {
        boolean b = xsbDataParser.validateHeader("");
        assertEquals(false, b);
    }

    @Test
    void testInvalidHeaderExtraField() {
        String invalidHeader = "extra~|~contractNumber~|~modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";
        assertEquals(false, xsbDataParser.validateHeader(invalidHeader));
    }


    @Test
    void testInvalidHeaderMissingField() {
        String invalidHeader = "modificationNumber~|~vendorName~|~vendorPartNumber~|~vendorDescription~|~bpaNumber~|~solicitationNumber~|~dunsNumber~|~sin~|~sinInference~|~sinsMatch~|~manufacturerName~|~manufacturerPartNumber~|~quantityOfUnit~|~quantityPerUnit~|~unitOfIssue~|~standardizedManufacturerName~|~standardizedManufacturerPartNumber~|~standardizedUnitOfIssue~|~fsc~|~nsn~|~unspsc~|~globalPackagingIdentifier~|~standardizedGlobalPackagingIdentifier~|~productName~|~productType~|~standardizedProductName~|~standardizedProductDescription~|~uniqueItemIdentifier~|~standardizedPackageQuantity~|~hits~|~selfHits~|~abilityOneItem~|~bioPreferred~|~comprehensiveProcurementGuidelineCompliant~|~significantNewAlternativesPolicyApproved~|~federalEnergyManagementProgramEnergyEfficientItem~|~waterSense~|~saferChoice~|~energyStar~|~epeat~|~primeItem~|~epaPrimaryMetalsFree~|~lowVoc~|~ets~|~deliveryFob~|~uniqueItemIdentifierScore~|~finalPrice~|~lowPriceTarget~|~midPriceTarget~|~highPriceTarget~|~catalogMinPrice~|~catalogAvgPrice~|~catalogMedianPrice~|~catalogMaxPrice~|~catalogPriceStandardDeviation~|~transactionMinPrice~|~transactionAvgPrice~|~transactionMedianPrice~|~transactionMaxPrice~|~catalogMinPriceSupplier~|~catalogMedianPriceSupplier~|~catalogMaxPriceSupplier~|~catalogMinPriceDelta~|~catalogAvgPriceDelta~|~catalogMedianPriceDelta~|~commercialCatalogLowPriceTarget~|~commercialCatalogMidPriceTarget~|~commercialCatalogHighPriceTarget~|~commercialCatalogMinPrice~|~commercialCatalogAvgPrice~|~commercialCatalogMedianPrice~|~commercialCatalogMaxPrice~|~commercialCatalogPriceStandardDeviation~|~commercialCatalogMinPriceSupplier~|~commercialCatalogMedianPriceSupplier~|~commercialCatalogMaxPriceSupplier~|~countryOriginInference~|~demandWeightedIndexScore~|~rankCategory~|~salesLikelihood~|~catalogMinPriceSource~|~catalogMedianPriceSource~|~catalogMaxPriceSource~|~isAuthorizedVendor~|~isProhibited~|~prohibitionCondition~|~prohibitionReason~|~prohibitionComment~|~fedmallMinPrice~|~fedmallMedPrice~|~fedmallAvgPrice~|~fedmallMaxPrice~|~nasaSewpMinPrice~|~nasaSewpMedPrice~|~nasaSewpAvgPrice~|~nasaSewpMaxPrice~|~vppSupplyCategoryId~|~vppIndicator~|~itemIdentifier~|~systemOfRecord~|~annualDemandQuantity~|~standardizedSinPrevalence~|~userDefinedInput~|~countryOrigin~|~unattributedManufacturerPartNumber~|~isInvalid~|~invalidReason~|~tdrAvgPrice~|~tdrMaxPrice~|~tdrMedianPrice~|~tdrMinPrice";
        assertEquals(false, xsbDataParser.validateHeader(invalidHeader));
    }


    @Test
    void parseXsbDataToArray() {
        String xsbData = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        String[] expected = {"47QSMA21D08R6","","AMERICAN SIGNAL COMPANY","","Verizon VPN with ITS Cloud Manager per year subscription, available for all models","","","612764845","NEW","NEW","true","AMERICAN SIGNAL COMPANY","OPT30125380","","1","EA","AMERICAN SIGNAL","OPT30125380","EA","","","","","","VERIZON VPN WITH ITS CLOUD MANAGER PER Y","","VERIZON VPN WITH ITS CLOUD MANAGER PER Y","Verizon VPN with ITS Cloud Manager per year subscription, available for all models","91580958","1","1","1","","false","false","false","false","false","false","false","false","false","false","false","false","PP","","344.58","344.58","390.93","437.27","344.58","344.58","344.58","344.58","0.0","0.0","0.0","0.0","0.0","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","0.0","0.0","0.0","","","","","","","","","","","","","0.00","Unknown","Unknown","gsa","gsa","gsa","9","false","","","","","","","","","","","","","","","","","100.00","","US","false","false","","","","",""};
        assertArrayEquals(expected, xsbDataParser.parseXsbDataToArray(xsbData));
        //assertTrue(Arrays.equals(expected, xsbDataParser.parseXsbDataToArray(xsbData)));
    }

    @Test
    void parseInvalidXsbDataToArray() {
        String xsbData = "~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(xsbData));
    }


    @Test
    void parseInvalidXsbDataToArrayExtraField() {
        String xsbData = "extra~|~47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(xsbData));
    }

    @Test
    void parseInvalidXsbDataInvalidDelimiter() {
        String xsbData = "47QSMA21D08R6,,AMERICAN SIGNAL COMPANY,,Verizon VPN with ITS Cloud Manager per year subscription, available for all models,,,612764845,NEW,NEW,true,AMERICAN SIGNAL COMPANY,OPT30125380,,1,EA,AMERICAN SIGNAL,OPT30125380,EA,,,,,,VERIZON VPN WITH ITS CLOUD MANAGER PER Y,,VERIZON VPN WITH ITS CLOUD MANAGER PER Y,Verizon VPN with ITS Cloud Manager per year subscription, available for all models,91580958,1,1,1,,false,false,false,false,false,false,false,false,false,false,false,false,PP,,344.58,344.58,390.93,437.27,344.58,344.58,344.58,344.58,0.0,0.0,0.0,0.0,0.0,AMERICAN SIGNAL COMPANY 47QSMA21D08R6,AMERICAN SIGNAL COMPANY 47QSMA21D08R6,AMERICAN SIGNAL COMPANY 47QSMA21D08R6,0.0,0.0,0.0,,,,,,,,,,,,,0.00,Unknown,Unknown,gsa,gsa,gsa,9,false,,,,,,,,,,,,,,,,,100.00,,US,false,false,,,,,";        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(xsbData));
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(xsbData));
    }

    @Test
    void parseInvalidXsbDataNullString() {
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(null));
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbDataToArray(""));

    }


    @Test
    void parseXsbDataToMap() {
        String[] expectedHeader = {"contractNumber","modificationNumber","vendorName","vendorPartNumber","vendorDescription","bpaNumber","solicitationNumber","dunsNumber","sin","sinInference","sinsMatch","manufacturerName","manufacturerPartNumber","quantityOfUnit","quantityPerUnit","unitOfIssue","standardizedManufacturerName","standardizedManufacturerPartNumber","standardizedUnitOfIssue","fsc","nsn","unspsc","globalPackagingIdentifier","standardizedGlobalPackagingIdentifier","productName","productType","standardizedProductName","standardizedProductDescription","uniqueItemIdentifier","standardizedPackageQuantity","hits","selfHits","abilityOneItem","bioPreferred","comprehensiveProcurementGuidelineCompliant","significantNewAlternativesPolicyApproved","federalEnergyManagementProgramEnergyEfficientItem","waterSense","saferChoice","energyStar","epeat","primeItem","epaPrimaryMetalsFree","lowVoc","ets","deliveryFob","uniqueItemIdentifierScore","finalPrice","lowPriceTarget","midPriceTarget","highPriceTarget","catalogMinPrice","catalogAvgPrice","catalogMedianPrice","catalogMaxPrice","catalogPriceStandardDeviation","transactionMinPrice","transactionAvgPrice","transactionMedianPrice","transactionMaxPrice","catalogMinPriceSupplier","catalogMedianPriceSupplier","catalogMaxPriceSupplier","catalogMinPriceDelta","catalogAvgPriceDelta","catalogMedianPriceDelta","commercialCatalogLowPriceTarget","commercialCatalogMidPriceTarget","commercialCatalogHighPriceTarget","commercialCatalogMinPrice","commercialCatalogAvgPrice","commercialCatalogMedianPrice","commercialCatalogMaxPrice","commercialCatalogPriceStandardDeviation","commercialCatalogMinPriceSupplier","commercialCatalogMedianPriceSupplier","commercialCatalogMaxPriceSupplier","countryOriginInference","demandWeightedIndexScore","rankCategory","salesLikelihood","catalogMinPriceSource","catalogMedianPriceSource","catalogMaxPriceSource","isAuthorizedVendor","isProhibited","prohibitionCondition","prohibitionReason","prohibitionComment","fedmallMinPrice","fedmallMedPrice","fedmallAvgPrice","fedmallMaxPrice","nasaSewpMinPrice","nasaSewpMedPrice","nasaSewpAvgPrice","nasaSewpMaxPrice","vppSupplyCategoryId","vppIndicator","itemIdentifier","systemOfRecord","annualDemandQuantity","standardizedSinPrevalence","userDefinedInput","countryOrigin","unattributedManufacturerPartNumber","isInvalid","invalidReason","tdrAvgPrice","tdrMaxPrice","tdrMedianPrice","tdrMinPrice"};
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        String[] xsbData = {"47QSMA21D08R6","","AMERICAN SIGNAL COMPANY","","Verizon VPN with ITS Cloud Manager per year subscription, available for all models","","","612764845","NEW","NEW","true","AMERICAN SIGNAL COMPANY","OPT30125380","","1","EA","AMERICAN SIGNAL","OPT30125380","EA","","","","","","VERIZON VPN WITH ITS CLOUD MANAGER PER Y","","VERIZON VPN WITH ITS CLOUD MANAGER PER Y","Verizon VPN with ITS Cloud Manager per year subscription, available for all models","91580958","1","1","1","","false","false","false","false","false","false","false","false","false","false","false","false","PP","","344.58","344.58","390.93","437.27","344.58","344.58","344.58","344.58","0.0","0.0","0.0","0.0","0.0","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","AMERICAN SIGNAL COMPANY 47QSMA21D08R6","0.0","0.0","0.0","","","","","","","","","","","","","0.00","Unknown","Unknown","gsa","gsa","gsa","9","false","","","","","","","","","","","","","","","","","100.00","","US","false","false","","","","",""};
        Map<String, String> expected = IntStream.range(0, expectedHeader.length)
                .boxed()
                .collect(Collectors.toMap(k -> expectedHeader[k], v -> xsbData[v]));
        assertTrue(expected.equals(xsbDataParser.parseXsbDataToMap(xsbDataString)));
    }

    @Test
    void parseXsbDataInvalidTaaContryCodes() {
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbData("testFile.gsa", xsbDataString, null));
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbData("testFile.gsa", xsbDataString, new ArrayList<>()));

    }

    @Test
    void parseXsbDataInvalidSourceFileName() {
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbData(null, xsbDataString, taaCountryCodes));
        assertThrows(IllegalArgumentException.class, () -> xsbDataParser.parseXsbData("", xsbDataString, taaCountryCodes));
    }

    @Test
    void parseXsbData() {
        String xsbDataString = "47QSMA21D08R6~|~~|~AMERICAN SIGNAL COMPANY~|~~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~~|~~|~612764845~|~NEW~|~NEW~|~true~|~AMERICAN SIGNAL COMPANY~|~OPT30125380~|~~|~1~|~EA~|~AMERICAN SIGNAL~|~OPT30125380~|~EA~|~~|~~|~~|~~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~~|~VERIZON VPN WITH ITS CLOUD MANAGER PER Y~|~Verizon VPN with ITS Cloud Manager per year subscription, available for all models~|~91580958~|~1~|~1~|~1~|~~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~false~|~PP~|~~|~344.58~|~344.58~|~390.93~|~437.27~|~344.58~|~344.58~|~344.58~|~344.58~|~0.0~|~0.0~|~0.0~|~0.0~|~0.0~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~AMERICAN SIGNAL COMPANY 47QSMA21D08R6~|~0.0~|~0.0~|~0.0~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~0.00~|~Unknown~|~Unknown~|~gsa~|~gsa~|~gsa~|~9~|~false~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~~|~100.00~|~~|~US~|~false~|~false~|~~|~~|~~|~~|~";
        XsbData xsbData = xsbDataParser.parseXsbData("testFile.gsa", xsbDataString, taaCountryCodes);
        assertEquals("47QSMA21D08R6", xsbData.getContractNumber());
        assertEquals("AMERICAN SIGNAL COMPANY", xsbData.getManufacturer());
        assertEquals("OPT30125380", xsbData.getPartNumber());
    }
}