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
            xsbDataService.trigger(trigger);
            message = "\nTriggered\n";
        }
        catch (ConcurrentModificationException e){
            message = "\n"+e.getMessage()+"\n";
        }
        return Mono.just(message);
    }
}
