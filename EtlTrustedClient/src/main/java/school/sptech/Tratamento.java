package school.sptech;

import software.amazon.awssdk.services.s3.model.*;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class Tratamento {

    private static final String TRUSTED_BUCKET = "trusted-nextrail-teste";
    private static final String CLIENT_BUCKET  = "client-nextrail-teste";

    private static final S3Service s3 = new S3Service();

    public static void main(String[] args) throws Exception {
        copiarEstrutura();
        PrevisoesTrat.processarPrevisoes();
        TratamentoComponente.processarComponentes();
    }

    public static void copiarEstrutura() throws Exception {

        LocalDate hoje = LocalDate.now();
        String hojeStr = hoje.toString();

        try {
            // empresas
            ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

            for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
                String empresa = empresaPrefix.prefix();

                ListObjectsV2Response servidores =
                        s3.listarComPrefixo(TRUSTED_BUCKET, empresa, true);

                for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {
                    String servidor = servidorPrefix.prefix();

                    ListObjectsV2Response arquivos =
                            s3.listarComPrefixo(TRUSTED_BUCKET, servidor, false);

                    for (S3Object arquivo : arquivos.contents()) {

                        if (!arquivo.key().contains("coleta_" + hojeStr + ".csv")) {
                            continue;
                        }

                        // Baixar CSV do dia
                        String csv = s3.baixarArquivo(TRUSTED_BUCKET, arquivo.key()).asUtf8String();

                        // Converter CSV do dia
                        List<Map<String, String>> dadosDoDia = CsvConverter.csvToList(csv);

                        // Mensal

                        String chaveMensal = MontarKey.gerarMensalKey(arquivo.key(), hoje);

                        // Carregar JSON mensal existente (se houver)
                        List<Map<String, String>> existenteMensal = s3.baixarJsonLista(CLIENT_BUCKET, chaveMensal);

                        // Junta os dois
                        existenteMensal.addAll(dadosDoDia);

                        // Envia para o S3
                        s3.enviarJsonLista(CLIENT_BUCKET, chaveMensal, existenteMensal);

                        System.out.println("MENSAL atualizado: " + chaveMensal);

                        // Anual

                        String chaveAnual = MontarKey.gerarAnualKey(arquivo.key(), hoje);

                        List<Map<String, String>> existenteAnual = s3.baixarJsonLista(CLIENT_BUCKET, chaveAnual);

                        existenteAnual.addAll(dadosDoDia);

                        s3.enviarJsonLista(CLIENT_BUCKET, chaveAnual, existenteAnual);

                        System.out.println("ANUAL atualizado: " + chaveAnual);
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Arquivo do dia ainda não foi gerado!");
        }
    }
}
