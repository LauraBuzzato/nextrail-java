package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.model.*;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class Tratamento implements RequestHandler<Map<String, Object>, String> {

    private static final String TRUSTED_BUCKET = "nextrail-trusted-log";
    private static final String CLIENT_BUCKET = "nextrail-client-log";

    private static final S3Service s3 = new S3Service();

    public String handleRequest(Map<String, Object> input, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Start");

        try {
            if (isJiraEvent(input)) {
                logger.log("Executando apenas JiraInfo.jiraMain()");
                JiraInfo.jiraMain();
                logger.log("Jira finalizado!");
                return "JiraInfo executado com sucesso!";
            } else {
                String resultado = realizarFluxoCompleto();
                logger.log("finalizado!");
                return resultado;
            }
        } catch (Exception e) {
            logger.log("Erro na realização da ETL2" + e.getMessage());
            return "ERRO";
        }
    }

    private boolean isJiraEvent(Map<String, Object> input) {
        if (input == null || input.isEmpty()) {
            return false;
        }

        if (input.containsKey("detail-type") && input.containsKey("source")) {
            String source = (String) input.get("source");
            String detailType = (String) input.get("detail-type");

            if ("aws.events".equals(source) && "Scheduled Event".equals(detailType)) {
                Object resourcesObj = input.get("resources");
                if (resourcesObj instanceof List) {
                    List<?> resources = (List<?>) resourcesObj;
                    for (Object resource : resources) {
                        if (resource instanceof String) {
                            String arn = (String) resource;
                            if (arn.contains("jira-nextrail")) {
                                return true;
                            }
                        }
                    }
                }
            }
        }

        return false;
    }

    private String realizarFluxoCompleto() throws Exception {
        copiarEstrutura();
        PrevisoesTrat.processarPrevisoes();
        TratamentoComponente.processarComponentes();
        AlertasTrat.processarAlertas();
        return "Sucesso!";
    }

    public static void copiarEstrutura() throws Exception {
        LocalDate hoje = LocalDate.now();
        String hojeStr = hoje.toString();

        try {
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

                        String csv = s3.baixarArquivo(TRUSTED_BUCKET, arquivo.key()).asUtf8String();

                        List<Map<String, String>> dadosDoDia = CsvConverter.csvToList(csv);

                        String chaveMensal = MontarKey.gerarMensalKey(arquivo.key(), hoje);

                        List<Map<String, String>> existenteMensal = s3.baixarJsonLista(CLIENT_BUCKET, chaveMensal);

                        existenteMensal.addAll(dadosDoDia);

                        s3.enviarJsonLista(CLIENT_BUCKET, chaveMensal, existenteMensal);

                        System.out.println("MENSAL atualizado: " + chaveMensal);

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