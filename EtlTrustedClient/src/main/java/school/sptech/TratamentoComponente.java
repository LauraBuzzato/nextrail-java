package school.sptech;

import software.amazon.awssdk.services.s3.model.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

public class TratamentoComponente {

    private static final String TRUSTED_BUCKET = "bucket-nextrail-trusted";
    private static final String CLIENT_BUCKET = "bucket-nextrail-client";

    private static final S3Service s3 = new S3Service();


    // ============================================================
    //   PROCESSAMENTO GERAL — LÊ CSV E EXPORTA PARA BUCKET CLIENT
    // ============================================================
    public static void processarComponentes() {

        try {
            ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

            for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {

                String empresa = empresaPrefix.prefix().replace("/", "");

                ListObjectsV2Response servidores =
                        s3.listarComPrefixo(TRUSTED_BUCKET, empresa + "/", true);

                for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {

                    String servidor = servidorPrefix.prefix()
                            .replace(empresa + "/", "")
                            .replace("/", "");

                    processarServidor(empresa, servidor);
                }
            }

        } catch (Exception e) {
            System.out.println("Erro em processarComponentes: " + e.getMessage());
        }
    }


    private static void processarServidor(String empresa, String servidor) {

        String prefix = String.format("%s/%s/", empresa, servidor);

        try {
            ListObjectsV2Response arquivos =
                    s3.listarComPrefixo(TRUSTED_BUCKET, prefix, false);

            for (S3Object arquivo : arquivos.contents()) {

                String key = arquivo.key();

                if (!key.endsWith(".csv")) continue;

                try {
                    String csv = s3.baixarArquivo(TRUSTED_BUCKET, key).asUtf8String();

                    List<ComponenteModel> dados =
                            ComponenteModel.parseCsv(csv, empresa, servidor);

                    if (!dados.isEmpty()) {
                        salvarNoClient(dados, empresa, servidor, key);
                    }

                } catch (Exception e) {
                    System.out.println("Erro ao ler arquivo: " + key);
                }
            }

        } catch (Exception e) {
            System.out.println("Erro ao processar servidor " + servidor);
        }
    }


    private static void salvarNoClient(List<ComponenteModel> dados,
                                       String empresa, String servidor, String keyOrigem) {

        try {
            String dataArquivo = extrairData(keyOrigem);
            String dataHoje = LocalDate.now()
                    .format(DateTimeFormatter.ofPattern("dd-MM-yyyy"));

            String keyOutput = String.format(
                    "%s/%s/componentes/dadosComponentes_%s_%s.json",
                    empresa, servidor, dataArquivo, dataHoje);

            s3.enviarJsonObject(CLIENT_BUCKET, keyOutput, dados);

        } catch (Exception e) {
            System.out.println("Erro ao enviar JSON para client: " + e.getMessage());
        }
    }


    private static String extrairData(String key) {
        try {
            String nome = key.substring(key.lastIndexOf("/") + 1);
            return nome.replace("coleta_", "").replace(".csv", "");
        } catch (Exception e) {
            return "data-desconhecida";
        }
    }
}
