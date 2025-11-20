package school.sptech;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class S3Service {

    private final S3Client s3;

    public S3Service() {
        this.s3 = S3Client.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(ProfileCredentialsProvider.create())
                .build();
    }

    public ListObjectsV2Response listarPastas(String bucket) {
        return s3.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .delimiter("/")
                .build());
    }

    public ListObjectsV2Response listarComPrefixo(String bucket, String prefix, boolean somentePastas) {
        ListObjectsV2Request.Builder b = ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(prefix);

        if (somentePastas)
            b.delimiter("/");

        return s3.listObjectsV2(b.build());
    }

    public ResponseBytes<GetObjectResponse> baixarArquivo(String bucket, String key) {
        return s3.getObjectAsBytes(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build());
    }

    // baixar lista pro json

    public List<Map<String, String>> baixarJsonLista(String bucket, String key) {
        try {
            ResponseBytes<GetObjectResponse> obj =
                    s3.getObjectAsBytes(GetObjectRequest.builder()
                            .bucket(bucket)
                            .key(key)
                            .build());

            String json = obj.asUtf8String();
            ObjectMapper m = new ObjectMapper();

            return m.readValue(json, new TypeReference<List<Map<String, String>>>() {});
        } catch (Exception e) {
            return new ArrayList<>();
        }
    }

    // enviar Json

    public void enviarJsonLista(String bucket, String key, List<Map<String, String>> dados) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String json = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(dados);

        s3.putObject(PutObjectRequest.builder()
                        .bucket(bucket)
                        .key(key)
                        .contentType("application/json")
                        .build(),
                RequestBody.fromString(json)
        );
    }
}
