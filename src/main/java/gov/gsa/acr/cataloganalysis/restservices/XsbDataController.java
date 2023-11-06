package gov.gsa.acr.cataloganalysis.restservices;

import gov.gsa.acr.cataloganalysis.model.Trigger;
import gov.gsa.acr.cataloganalysis.service.XsbDataService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.ExampleObject;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ConcurrentModificationException;

@Slf4j
@RestController
@Tag(name= "ACR Catalog Analysis Service", description = "A Service for analyzing catalogs.")
public class XsbDataController extends BaseController{
    final XsbDataService xsbDataService;

    public XsbDataController(XsbDataService xsbDataService) {
        this.xsbDataService = xsbDataService;
    }

    @Operation(summary = "Trigger (start) the XSB data upload process.",
            description = """
    This API endpoint is used to trigger the Catalog Analysis Service to start loading the bi-monthly XSB data. The end point expects
    a trigger message. The examples listed below explains the structure of the trigger message and explains what each attribute means.
    It is important to read the description for each example to understand the nuances for different options for getting XSB source files.
    """
    )
    @PostMapping(value="/trigger", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Mono<String> trigger(@io.swagger.v3.oas.annotations.parameters.RequestBody(content = {
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
                                    {"sourceType": "SFTP",
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
                                    "sourceFolder":"testData",
                                     "files": ["47QSMA21D08R6-7000039_20230919195858_4325137760202194341_report_1.gsa", "47QSWA19D0073-3003521"]
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
                                    "sourceFolder":"/app/testData",
                                     "files": ["47QSMA21D08R6-7000039_20230919195858_4325137760202194341_report_1.gsa", "47QSWA19D0073-3003521"]
                                    }
                                    """
                    )
            })})

                                    @RequestBody Trigger trigger){
        log.info("Request body " + trigger);
        String message;
        try {
            xsbDataService.triggerDataUpload(trigger).subscribe(null, e -> log.error("Unexpected Error", e));
            message = "\nTriggered\n";
        }
        catch (ConcurrentModificationException e){
            message = "\n"+e.getMessage()+"\n";
        }
        return Mono.just(message);
    }



    @Operation(summary = "Trigger (start) the SFTP file download process.",
            description = """
    This API endpoint is used to trigger the Catalog Analysis Service to start downloading the bi-monthly XSB report files from XSB. The end point expects
    a trigger message. The examples listed below explains the structure of the trigger message and explains what each attribute means.
    It is important to read the description for each example to understand the nuances for different options for getting XSB source files.
    """
    )
    @PostMapping(value = "/download", produces = MediaType.APPLICATION_NDJSON_VALUE)
    public Flux<String> sftp(@io.swagger.v3.oas.annotations.parameters.RequestBody(content = {
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
                                            {"sourceType": "SFTP",
                                             "files": ["47QSMA21D08R6-7000039_20230919195858_4325137760202194341_report_1.gsa",
                                             "47QSEA19D00BV-3000176_20231010142725_1261618939108793341_report_1.gsa",
                                             "47QSEA18D0085-3008870_20231010180915_2847181293179224282_report_1.gsa",
                                             "47QSMA19D08P6-3008918_20231023124520_8813056341555794877_report_1.gsa",
                                             "47QSCA18D000Z-3003661_20231025122311_3530777008603647906_report_1.gsa",
                                             "47QSWA19D0073-3003521_20230920163659_5714997144374137074_report_1.gsa"
                                             ]
                                            }
                                            """
                            )
                            ,
                            @ExampleObject(
                                    name = "Sample Trigger Object from SFTP server - 2",
                                    summary = "SFTP Example",
                                    description = """
                                            To trigger downloading files from XSB's SFTP server. The sourceFolder attribute  is not required as the
                                            default value is configured already in the system. The "files" property could be an array of full file names
                                            or glob like patterns.
                                            """,
                                    value = """
                                            {"sourceType": "SFTP",
                                            "sourceFolder": "/ACR/catalog_upload",
                                             "files": [
                                             "47QSEA19D007N-3008951_20231031174025.gsa",
                                             "47QSCA18D000Z-3008957_20231101135517.gsa"
                                             ]
                                            }
                                            """
                            )
                    })})

                             @RequestBody Trigger trigger) {
        if (trigger == null) return Flux.error(new IllegalArgumentException("Invalid request body in POST"));
        log.info("Trigger: " + trigger);
        try {
            return xsbDataService.downloadReports(trigger)
                    .map(String::valueOf)
                    .map(s -> s + "\n")
                    .doOnNext(s -> log.info("File file file " + s));
            //return Flux.just("Triggered");

        } catch (ConcurrentModificationException e) {
            return Flux.just(e.getMessage());
        }
    }

}
