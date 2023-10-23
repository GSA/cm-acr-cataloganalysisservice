package gov.gsa.acr.cataloganalysis.configuration;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import software.amazon.awssdk.regions.Region;

import java.net.URI;

@ConfigurationProperties(prefix = "aws.s3")
@Data
public class S3ClientConfigurarionProperties {

    private Region region = Region.US_EAST_1;
    private URI endpoint = null;

    private String key;
    private String secret;

    // Bucket name we'll be using as our backend storage
    private String bucket;

    private String baseDir;
}
