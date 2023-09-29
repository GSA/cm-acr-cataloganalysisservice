package gov.gsa.acr.cataloganalysis.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import lombok.Data;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonPropertyOrder({
        "catalogMedianPrice",
        "catalogMedianPriceSupplier",
        "catalogMinPrice",
        "catalogMinPriceSupplier",
        "catalogPriceStandardDeviation",
        "commercialCatalogMedianPrice",
        "commercialCatalogMedianPriceSupplier",
        "commercialCatalogMinPrice",
        "commercialCatalogMinPriceSupplier",
        "countryOriginInference",
        "demandWeightedIndexScore",
        "dunsNumber",
        "enrichment_lower_bound",
        "enrichment_upper_bound",
        "ets",
        "exceeds_market_threshold",
        "finalPrice",
        "gsaFeedDate",
        "highPriceTarget",
        "hits",
        "invalidReason",
        "is_low_outlier",
        "is_market_research_found",
        "is_mia_risk",
        "is_taa_risk",
        "isAuthorizedVendor",
        "isProhibited",
        "lowPriceTarget",
        "nsn",
        "prohibitionComment",
        "salesLikelihood",
        "selfHits",
        "standardizedManufacturerName",
        "standardizedManufacturerPartNumber",
        "standardizedProductDescription",
        "standardizedUnitOfIssue",
        "tdrMedianPrice",
        "tdrMinPrice",
        "tdrMaxPrice",
        "uniqueItemIdentifier",
        "vppIndicator",
        "prohibitionCondition",
        "prohibitionReason",
        "rankCategory",
        "country_of_origin",
        "ppapi_invalid_fields",
        "modificationNumber",
        "solicitationNumber",
        "abilityOneItem",
        "globalPackagingIdentifier",
        "vendorName",
        "vendorDescription",
        "quantityOfUnit",
        "quantityPerUnit",
        "productName",
        "unspsc",
        "unitOfIssue",
        "productType",
        "deliveryFob",
        "fsc"
})
@Data
public class XsbDataJsonRecord {
    @JsonProperty("catalogMedianPrice")
    private Double catalogMedianPrice;
    @JsonProperty("catalogMedianPriceSupplier")
    private String catalogMedianPriceSupplier;
    @JsonProperty("catalogMinPrice")
    private Double catalogMinPrice;
    @JsonProperty("catalogMinPriceSupplier")
    private String catalogMinPriceSupplier;
    @JsonProperty("catalogPriceStandardDeviation")
    private Double catalogPriceStandardDeviation;
    @JsonProperty("commercialCatalogMedianPrice")
    private Double commercialCatalogMedianPrice;
    @JsonProperty("commercialCatalogMedianPriceSupplier")
    private String commercialCatalogMedianPriceSupplier;
    @JsonProperty("commercialCatalogMinPrice")
    private Double commercialCatalogMinPrice;
    @JsonProperty("commercialCatalogMinPriceSupplier")
    private String commercialCatalogMinPriceSupplier;
    @JsonProperty("countryOriginInference")
    private String countryOriginInference;
    @JsonProperty("demandWeightedIndexScore")
    private Double demandWeightedIndexScore;
    @JsonProperty("dunsNumber")
    private String dunsNumber;
    @JsonProperty("enrichment_lower_bound")
    private Double enrichmentLowerBound;
    @JsonProperty("enrichment_upper_bound")
    private Double enrichmentUpperBound;
    @JsonProperty("ets")
    private Boolean ets;
    @JsonProperty("exceeds_market_threshold")
    private Boolean exceedsMarketThreshold;
    @JsonProperty("finalPrice")
    private Double finalPrice;
    @JsonProperty("gsaFeedDate")
    private String gsaFeedDate;
    @JsonProperty("highPriceTarget")
    private Double highPriceTarget;
    @JsonProperty("hits")
    private Integer hits;
    @JsonProperty("invalidReason")
    private String invalidReason;
    @JsonProperty("is_low_outlier")
    private Boolean isLowOutlier;
    @JsonProperty("is_market_research_found")
    private Boolean isMarketResearchFound;
    @JsonProperty("is_mia_risk")
    private Boolean isMiaRisk;
    @JsonProperty("is_taa_risk")
    private Boolean isTaaRisk;
    @JsonProperty("isAuthorizedVendor")
    private Integer isAuthorizedVendor;
    @JsonProperty("isProhibited")
    private Boolean isProhibited;
    @JsonProperty("lowPriceTarget")
    private Double lowPriceTarget;
    @JsonProperty("nsn")
    private String nsn;
    @JsonProperty("prohibitionComment")
    private String prohibitionComment;
    @JsonProperty("salesLikelihood")
    private String salesLikelihood;
    @JsonProperty("selfHits")
    private Integer selfHits;
    @JsonProperty("standardizedManufacturerName")
    private String standardizedManufacturerName;
    @JsonProperty("standardizedManufacturerPartNumber")
    private String standardizedManufacturerPartNumber;
    @JsonProperty("standardizedProductDescription")
    private String standardizedProductDescription;
    @JsonProperty("standardizedUnitOfIssue")
    private String standardizedUnitOfIssue;
    @JsonProperty("tdrMedianPrice")
    private Double tdrMedianPrice;
    @JsonProperty("tdrMinPrice")
    private Double tdrMinPrice;
    @JsonProperty("tdrMaxPrice")
    private Double tdrMaxPrice;
    @JsonProperty("uniqueItemIdentifier")
    private String uniqueItemIdentifier;
    @JsonProperty("vppIndicator")
    private Integer vppIndicator;
    @JsonProperty("prohibitionCondition")
    private String prohibitionCondition;
    @JsonProperty("prohibitionReason")
    private String prohibitionReason;
    @JsonProperty("rankCategory")
    private String rankCategory;
    @JsonProperty("country_of_origin")
    private String countryOfOrigin;
    @JsonProperty("ppapi_invalid_fields")
    private List<String> ppapiInvalidFields;
    @JsonProperty("modificationNumber")
    private String modificationNumber;
    @JsonProperty("solicitationNumber")
    private String solicitationNumber;
    @JsonProperty("abilityOneItem")
    private String abilityOneItem;
    @JsonProperty("globalPackagingIdentifier")
    private String globalPackagingIdentifier;
    @JsonProperty("vendorName")
    private String vendorName;
    @JsonProperty("vendorDescription")
    private String vendorDescription;
    @JsonProperty("quantityOfUnit")
    private Integer quantityOfUnit;
    @JsonProperty("quantityPerUnit")
    private Integer quantityPerUnit;
    @JsonProperty("vendorPartNumber")
    private String vendorPartNumber;
    @JsonProperty("productName")
    private String productName;
    @JsonProperty("unspsc")
    private String unspsc;
    @JsonProperty("unitOfIssue")
    private String unitOfIssue;
    @JsonProperty("productType")
    private String productType;
    @JsonProperty("deliveryFob")
    private String deliveryFob;
    @JsonProperty("fsc")
    private String fsc;
    @JsonProperty("annualDemandQuantity")
    private Integer annualDemandQuantity;
    @JsonProperty("catalogAvgPrice")
    private Double catalogAvgPrice;
    @JsonProperty("catalogMaxPrice")
    private Double catalogMaxPrice;
    @JsonProperty("transactionMinPrice")
    private Double transactionMinPrice;
    @JsonProperty("transactionAvgPrice")
    private Double transactionAvgPrice;
    @JsonProperty("transactionMedianPrice")
    private Double transactionMedianPrice;
    @JsonProperty("transactionMaxPrice")
    private Double transactionMaxPrice;

    public XsbDataJsonRecord(Map<String, String> xsbDataAsAMap) {
        // Refer: See https://docs.google.com/spreadsheets/d/1YuZpJOBl9jkHgciPDsEkNmGiG5NBcuauSDU76lQvbEU/view#gid=173420408
        if (xsbDataAsAMap == null) throw new NullPointerException("Cannot convert a NULL Map to XSB Data");
        String ls = System.getProperty("line.separator");
        StringBuffer sb = new StringBuffer();

        this.setModificationNumber(xsbDataAsAMap.get("modificationNumber")); //modificationNumber
        this.setSolicitationNumber(xsbDataAsAMap.get("solicitationNumber")); //solicitationNumber
        this.setDunsNumber(xsbDataAsAMap.get("dunsNumber")); //dunsNumber
        try {
            if (xsbDataAsAMap.get("quantityPerUnit") != null)
                this.setQuantityPerUnit(Integer.valueOf(xsbDataAsAMap.get("quantityPerUnit"))); //quantityPerUnit
        }
        catch (Exception e) {
            sb.append("Invalid data, Quantity Per Unit. Must be a valid number. Value encountered: " + xsbDataAsAMap.get("quantityPerUnit"));
            sb.append(ls);
        }
        this.setAbilityOneItem(xsbDataAsAMap.get("abilityOneItem")); //abilityOneItem
        this.setGlobalPackagingIdentifier(xsbDataAsAMap.get("globalPackagingIdentifier")); //globalPackagingIdentifier
        this.setVendorName(xsbDataAsAMap.get("vendorName")); //vendorName
        this.setVendorDescription(xsbDataAsAMap.get("vendorDescription")); //vendorDescription
        this.setProductName(xsbDataAsAMap.get("productName")); //productName
        this.setUnspsc(xsbDataAsAMap.get("unspsc")); //unspsc
        this.setUnitOfIssue(xsbDataAsAMap.get("unitOfIssue")); //unitOfIssue
        this.setProductType(xsbDataAsAMap.get("productType")); //productType
        this.setDeliveryFob(xsbDataAsAMap.get("deliveryFob")); //deliveryFob
        try {
            if (xsbDataAsAMap.get("finalPrice") != null)
                this.setFinalPrice(Double.valueOf(xsbDataAsAMap.get("finalPrice"))); //finalPrice
        }
        catch (Exception e) {
            sb.append("Invalid data, Final Price. Must be a valid number. Value encountered: " + xsbDataAsAMap.get("finalPrice"));
            sb.append(ls);
        }
        this.setFsc(xsbDataAsAMap.get("fsc")); //fsc

        if (sb.length() > 0) throw new IllegalArgumentException(sb.toString());
    }
}
