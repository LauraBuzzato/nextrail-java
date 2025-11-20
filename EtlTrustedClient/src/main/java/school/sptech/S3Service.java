package school.sptech;

import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

public class S3Service {

    private static final S3Client s3 = S3Client.builder()
            .region(Region.US_EAST_1)
            .credentialsProvider(ProfileCredentialsProvider.create())
            .build();

    public ListObjectsV2Response listarPastas(String bucket) {
        return s3.listObjectsV2(
                ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .delimiter("/")
                        .build()
        );
    }

    public ListObjectsV2Response listarComPrefixo(String bucket, String prefixo, boolean usarDelimitador) {
        ListObjectsV2Request.Builder req = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefixo);

        if (usarDelimitador) req.delimiter("/");

        return s3.listObjectsV2(req.build());
    }

    public ResponseBytes<GetObjectResponse> baixarArquivo(String bucket, String key) {
        return s3.getObjectAsBytes(
                GetObjectRequest.builder().bucket(bucket).key(key).build()
        );
    }

    public void enviarJson(String bucket, String key, String json) {
        s3.putObject(
                PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("application/json")
                        .build(),
                RequestBody.fromString(json)
        );
    }
}
