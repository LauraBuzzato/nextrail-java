package school.sptech;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.core.sync.RequestBody;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

            String key = String.format("%s/%s/coleta_%s.csv",
                    empresaFolder, servidorFolder, dataAtual);

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketTrusted)
                            .key(key)
                            .contentType("text/csv")
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

    public LocalDate extrairDataDoNome(String fileName) {
        if (fileName == null || fileName.isEmpty()) {
            return null;
        }

        Pattern pattern = Pattern.compile("(\\d{4}-\\d{2}-\\d{2})");
        Matcher matcher = pattern.matcher(fileName);

        if (matcher.find()) {
            try {
                String dataStr = matcher.group(1);
                return LocalDate.parse(dataStr, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            } catch (DateTimeParseException e) {
                System.out.println("Data encontrada mas formato inválido: " + matcher.group(1));
            }
        }

        return null;
    }

    public S3Object encontrarArquivoMaisRecente(String bucketName, String prefixo) {
        try {
            ListObjectsV2Request request = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .prefix(prefixo)
                    .build();

            ListObjectsV2Response response = s3.listObjectsV2(request);

            if (response.contents().isEmpty()) {
                System.out.println("Nenhum arquivo encontrado no bucket: " + bucketName + " com prefixo: " + prefixo);
                return null;
            }

            S3Object arquivoMaisRecente = null;
            LocalDate dataMaisRecente = null;

            for (S3Object obj : response.contents()) {
                LocalDate dataArquivo = extrairDataDoNome(obj.key());
                if (dataArquivo != null) {
                    if (dataMaisRecente == null || dataArquivo.isAfter(dataMaisRecente)) {
                        dataMaisRecente = dataArquivo;
                        arquivoMaisRecente = obj;
                    }
                }
            }

            if (arquivoMaisRecente != null) {
                System.out.println("Arquivo mais recente encontrado: " + arquivoMaisRecente.key());
                System.out.println("Data do arquivo: " + dataMaisRecente);
            } else {
                System.out.println("Arquivos encontrados no bucket:");
                for (S3Object obj : response.contents()) {
                    System.out.println(" - " + obj.key());
                }
            }

            return arquivoMaisRecente;

        } catch (Exception e) {
            System.out.println("Erro ao buscar arquivo mais recente: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }


    public String gerarNomeComDataAtual() {
        String dataAtual = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        return "machine_data_" + dataAtual + ".csv";
    }

    public boolean ehArquivoDeHoje(String fileName) {
        LocalDate dataArquivo = extrairDataDoNome(fileName);
        return dataArquivo != null && dataArquivo.equals(LocalDate.now());
    }
}