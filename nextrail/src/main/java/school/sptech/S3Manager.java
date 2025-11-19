package school.sptech;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class S3Manager {

    private final S3Client s3;
    private final String bucketTrusted;

    public S3Manager(S3Client s3, String bucketTrusted) {
        this.s3 = s3;
        this.bucketTrusted = bucketTrusted;
    }

    public String salvarCSVTrusted(String empresaNome, String servidorNome, String csvContent) {
        try {

            String empresaFolder = formatarNome(empresaNome);
            String servidorFolder = formatarNome(servidorNome);


            String dataAtual = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

            String key = String.format("%s/%s/coleta_%s_%s.csv",
                    empresaFolder, servidorFolder, dataAtual);


            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketTrusted)
                            .key(key)
                            .build(),
                    RequestBody.fromString(csvContent)
            );

            System.out.println("Arquivo salvo em: s3://" + bucketTrusted + "/" + key);
            return key;

        } catch (Exception e) {
            System.out.println("Erro ao salvar arquivo no S3: " + e.getMessage());
            return null;
        }
    }

    private String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }
}