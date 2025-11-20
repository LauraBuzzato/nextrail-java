package school.sptech;

import software.amazon.awssdk.services.s3.model.*;

import java.time.LocalDate;

public class Tratamento {

    private static final String TRUSTED_BUCKET = "bucket-trusted-teste-tratamento";
    private static final String CLIENT_BUCKET = "bucket-client-teste-etl";

    private static final S3Service s3 = new S3Service();

    public static void main(String[] args) throws Exception {
        copiarEstrutura();
    }

    public static void copiarEstrutura() throws Exception {

        LocalDate hoje = LocalDate.now();
        String hojeStr = hoje.toString();

        // empresas
        ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

        for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
            String empresa = empresaPrefix.prefix();
            System.out.println("Empresa: " + empresa);

            // servidores da empresa
            ListObjectsV2Response servidores = s3.listarComPrefixo(TRUSTED_BUCKET, empresa, true);

            for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {
                String servidor = servidorPrefix.prefix();
                System.out.println("  Servidor: " + servidor);

                // arquivos dentro do servidor (CSV)
                ListObjectsV2Response arquivos =
                        s3.listarComPrefixo(TRUSTED_BUCKET, servidor, false);

                for (S3Object arquivo : arquivos.contents()) {

                    if (!arquivo.key().contains("coleta_" + hojeStr + ".csv")) {
                        continue;
                    }

                    System.out.println("    Convertendo: " + arquivo.key());

                    // Baixar CSV
                    String csv = s3.baixarArquivo(TRUSTED_BUCKET, arquivo.key()).asUtf8String();

                    // Converter CSV → JSON
                    String json = CsvConverter.csvToJson(csv);

                    // Criar JSON mensal
                    String jsonMensalKey = MontarKey.gerarMensalKey(arquivo.key(), hoje);
                    s3.enviarJson(CLIENT_BUCKET, jsonMensalKey, json);
                    System.out.println("    -> JSON MENSAL: " + jsonMensalKey);

                    // Criar JSON anual
                    String jsonAnualKey = MontarKey.gerarAnualKey(arquivo.key(), hoje);
                    s3.enviarJson(CLIENT_BUCKET, jsonAnualKey, json);
                    System.out.println("    -> JSON ANUAL: " + jsonAnualKey);
                }
            }
        }
    }
}
