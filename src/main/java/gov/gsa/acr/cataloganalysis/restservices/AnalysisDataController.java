package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.analysissource.AnalysisSourceXsb;
import gov.gsa.acr.cataloganalysis.model.DataUploadResults;
import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.model.XsbData;
import gov.gsa.acr.cataloganalysis.repositories.XsbDataRepository;
import gov.gsa.acr.cataloganalysis.scheduler.ScheduledTasks;
import gov.gsa.acr.cataloganalysis.service.AnalysisDataProcessingService;
import gov.gsa.acr.cataloganalysis.service.XsbPpApiService;
import io.swagger.v3.oas.annotations.Hidden;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Slf4j
@RestController
@Tag(name= "ACR Catalog Analysis Service", description = "A Service for analyzing catalogs.")
public class AnalysisDataController extends BaseController{
    final AnalysisDataProcessingService analysisDataProcessingService;
    private final XsbDataRepository xsbDataRepository;
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);

    // TBD delete this after the demo. Not needed.
    private final XsbPpApiService xsbPpApiService;
    private final AnalysisSourceXsb analysisSourceXsb;
    private final ScheduledTasks scheduledTasks;

    public AnalysisDataController(AnalysisDataProcessingService analysisDataProcessingService, XsbDataRepository xsbDataRepository, XsbPpApiService xsbPpApiService, AnalysisSourceXsb analysisSourceXsb, ScheduledTasks scheduledTasks) {
        this.analysisDataProcessingService = analysisDataProcessingService;
        this.xsbDataRepository = xsbDataRepository;
        this.xsbPpApiService = xsbPpApiService;
        this.analysisSourceXsb = analysisSourceXsb;
        this.scheduledTasks = scheduledTasks;
    }

    @Operation(summary = "Trigger (start) the XSB data upload process.",
            description = """
    This API endpoint is used to trigger the Catalog Analysis Service to start loading the bi-monthly XSB data. The end point expects
    a trigger message. The examples listed below explains the structure of the trigger message and explains what each attribute means.
    It is important to read the description for each example to understand the nuances for different options for getting XSB source files.
    """
    )
    @PostMapping(value="/trigger", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public ResponseEntity<Mono<String>> trigger(@io.swagger.v3.oas.annotations.parameters.RequestBody(content = {
            @Content(
            schema = @Schema(implementation = Trigger.class),
            examples = {
                    @ExampleObject(
                            name = "Sample Trigger Object from SFTP server",
                            summary = "SFTP Example",
                            description = """
                            To trigger downloading files from XSB's SFTP server. The sourceFolder attribute  is not required as the
                            default value is configured already in the system. The "files" property could be an array of full file names
                            or glob like patterns.
                            """,
                            value = """
                                    {"sourceType": "XSB",
                                     "gsaFeedDate": "2024-01-01",
                                     "files": ["47QSMA21D08R6-7000039_20230919195858_4325137760202194341_report_1.gsa", "47QSWA19D0073-3003521*"]
                                    }
                                    """
                                    ),
                    @ExampleObject(
                            name = "Sample Trigger Object from S3 bucket",
                            summary = "S3 Example",
                            description = """
                            To trigger downloading files from ACR's S3 bucket.
                            If the sourceFolder is provided, it is relative to "catalogAnalysis" on S3, meaning the base is always "catalogAnalysis", so in the example, the files will be looked relative to catalogAnalysis/testData/.
                            If the sourceFolder is not provided, the files will be looked under catalogAnalysis.
                            The "files" property could be an array of full file names
                            or prefix, since glob patterns do not work in case of S3.
                            """,
                            value = """
                                    {"sourceType": "S3",
                                    "sourceFolder":"junitTestData",
                                    "gsaFeedDate": "2024-01-01",
                                     "files": [ "47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa",
                                                "47QSWA18D000C-3008711"]
                                    }
                                    """
                    )
                    ,
                    @ExampleObject(
                            name = "Sample Trigger Object from the local file system",
                            summary = "Local Example",
                            description = """
                            To trigger downloading files from the pod's local file system.
                            SourceFolder is required in this case and it should be the absolute path to the files.
                            The "files" property could be an array of full file names
                            or glob like patterns.
                            """,
                            value = """
                                    {"sourceType": "LOCAL",
                                    "sourceFolder":"junitTestData",
                                    "gsaFeedDate": "2024-01-01",
                                     "files": ["47QSMA21D08R6-7000039_20230901135843_5367723946113572875_report_1.gsa"]
                                    }
                                    """
                    )
            })})

                                    @RequestBody Trigger trigger){
        log.info("Request body " + trigger);
        try {
            Mono<DataUploadResults> dataUploadResultsMono =  analysisDataProcessingService.triggerDataUpload(trigger);
            executorService.submit(() -> dataUploadResultsMono.subscribe(null, e -> log.error("Unexpected Error", e)));
            return ResponseEntity
                    .status(HttpStatus.OK)
                    .body(Mono.just("\nTriggered\n"));
        }
        catch (ConcurrentModificationException e){
            log.error("Process already executing", e);
            return ResponseEntity
                    .status(HttpStatus.SERVICE_UNAVAILABLE)
                    .body(Mono.just("\n"+e.getMessage()+"\n"));
        }
        catch (IllegalArgumentException e){
            log.error("The request is illegal.", e);
            return ResponseEntity
                    .status(HttpStatus.BAD_REQUEST)
                    .body(Mono.just("\n"+e.getMessage()+"\n"));
        }
        catch (Exception e) {
            log.error("Unexpected error", e);
            return ResponseEntity
                    .status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Mono.just("\n"+e.getMessage()+"\n"));
        }
    }


    @Operation(summary = "Get the number of products that have the ETS flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the ETS (Essentially The Same) flag set to true.
    """
    )
    @GetMapping(value="/ets/count")
    @Hidden
    public Mono<Integer> etsCount(){
        return xsbDataRepository.etsCount();
    }

    @Operation(summary = "Get all products that have the ETS flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the ETS (Essentially The Same) flag set to true.
    """
    )
    @GetMapping(value="/ets", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getETSProducts(){
        return xsbDataRepository.findAllETS();
    }

    @Operation(summary = "Get the number of products that have the is_low_outlier flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the is_low_outlier flag set to true.
    """
    )
    @GetMapping(value="/low-outlier/count")
    @Hidden
    public Mono<Integer> lowOutlierCount(){
        return xsbDataRepository.isLowOutlierCount();
    }

    @Operation(summary = "Get all products that have the is_low_outlier flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the is_low_ourlier flag set to true.
    """
    )
    @GetMapping(value="/low-outlier", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getLowOutlierProducts(){
        return xsbDataRepository.findAllLowOutliers();
    }


    @Operation(summary = "Get the number of products that have the is_mis_risk flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the is_mia_risk flag set to true.
    """
    )
    @GetMapping(value="/mia-risk/count")
    @Hidden
    public Mono<Integer> miaRiskCount(){
        return xsbDataRepository.isMIARiskCount();
    }

    @Operation(summary = "Get all products that have the is_mia_risk flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the is_mia_risk flag set to true.
    """
    )
    @GetMapping(value="/mia-risk", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getMiaRiskProducts(){
        return xsbDataRepository.findAllMIARisk();
    }

    @Operation(summary = "Get the number of products that have the exceeds_market_threshold flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the exceeds_market_threshold flag set to true.
    """
    )
    @GetMapping(value="/exceeds-market-threshold/count")
    @Hidden
    public Mono<Integer> exceedsMarketThresholdCount(){
        return xsbDataRepository.exceedsMarketThresholdCount();
    }

    @Operation(summary = "Get all products that have the exceeds_market_threshold flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the exceeds_market_threshold flag set to true.
    """
    )
    @GetMapping(value="/exceeds-market-threshold", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getExceedsMarketThresholdProducts(){
        return xsbDataRepository.findAllExceedsMarketThreshold();
    }

    @Operation(summary = "Get the number of products that have the isProhibited flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the isProhibited flag set to true.
    """
    )
    @GetMapping(value="/isProhibited/count")
    @Hidden
    public Mono<Integer> isProhibitedCount(){
        return xsbDataRepository.isProhibitedCount();
    }

    @Operation(summary = "Get all products that have the isProhibited flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the isProhibited flag set to true.
    """
    )
    @GetMapping(value="/isProhibited", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getProhibitedProducts(){
        return xsbDataRepository.findAllProhibitedProducts();
    }


    @Operation(summary = "Get the number of products that have the is_taa_risk flag set to true.",
            description = """
    This API endpoint is used to get the number of products that have the is_taa_risk flag set to true.
    """
    )
    @GetMapping(value="/taa-risk/count")
    @Hidden
    public Mono<Integer> taaRiskCount(){
        return xsbDataRepository.isTAARiskCount();
    }

    @Operation(summary = "Get all products that have the is_taa_risk flag set to true.",
            description = """
    This API endpoint is used to get all the products that have the is_taa_risk flag set to true.
    """
    )
    @GetMapping(value="/taa-risk", produces = MediaType.APPLICATION_NDJSON_VALUE)
    @Hidden
    public Flux<XsbData> getTaaRiskProducts(){
        return xsbDataRepository.findAllTAARisk();
    }

    // TBD All the methods below this comment should be deleted after the demp. These are temporary for demo purpose only.
    @GetMapping(value ="/acr-feed-date", produces = MediaType.TEXT_PLAIN_VALUE)
    public Mono<String> getLatestAcrFeedDate()
    {
        log.debug("Get latest ACR Feed Date enter");
        return this.xsbDataRepository.getAcrFeedDate();
    }

    @GetMapping(value ="/gsa-feed-date/{acrFeedDate}", produces = MediaType.TEXT_PLAIN_VALUE)
    public Mono<String> getLatestGsaFeedDate(
            @Parameter(description = "The ACR Feed Date")
            @PathVariable String acrFeedDate
    )
    {
        log.debug("Get latest GSA Feed date enter");
        return this.xsbPpApiService.getGsaFeedDate(acrFeedDate);
    }

    @GetMapping(value ="/report-names/{gsaFeedDate}")
    public List<String> getLatestBimonthlyFileNames(
            @Parameter(description = "The latest GSA Feed Date")
            @PathVariable String gsaFeedDate
    )
    {
        log.debug("Get latest GSA Feed date enter");
        return scheduledTasks.getNewSftpReportsName(gsaFeedDate)
                .stream().sorted(Comparator.reverseOrder())
                .collect(Collectors.toUnmodifiableList());

    }


    @GetMapping(value ="/report-names")
    public List<String> getAllBimonthlyFileNames()
    {
        log.debug("Get latest GSA Feed date enter");
        return analysisSourceXsb.getBimonthlyReportNames(null)
                .stream().sorted(Comparator.reverseOrder())
                .collect(Collectors.toUnmodifiableList());

    }


}
