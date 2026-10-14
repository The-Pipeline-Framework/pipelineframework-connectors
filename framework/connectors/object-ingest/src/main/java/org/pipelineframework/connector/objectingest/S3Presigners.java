package org.pipelineframework.connector.objectingest;

import java.util.Objects;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;

/** Builds transfer credentials with the identity and endpoint used for S3 object access. */
final class S3Presigners {
    private S3Presigners() { }

    static S3Presigner forClient(S3Client client, boolean pathStyleAccess) {
        var configuration = Objects.requireNonNull(client.serviceClientConfiguration(),
            "S3 client configuration is required for presigning");
        var builder = S3Presigner.builder()
            .region(Objects.requireNonNull(configuration.region(), "S3 client region is required for presigning"))
            .credentialsProvider(Objects.requireNonNull(configuration.credentialsProvider(),
                "S3 client credentials are required for presigning"))
            .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(pathStyleAccess).build());
        configuration.endpointOverride().ifPresent(builder::endpointOverride);
        return builder.build();
    }
}
