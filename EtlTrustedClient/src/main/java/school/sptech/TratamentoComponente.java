package school.sptech;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

public class TratamentoComponente {

    private static final String TRUSTED_BUCKET = "nextrail-trusted-log";
    private static final String CLIENT_BUCKET = "nextrail-client-log";

    private static final S3Service s3 = new S3Service();

    public static void processarComponentes() throws Exception {

        LocalDate hoje = LocalDate.now();
        String hojeStr = hoje.toString();

        System.out.println("Iniciando processamento de componentes...");

        // LISTAR empresas
        var empresas = s3.listarPastas(TRUSTED_BUCKET);

        for (var empresaPrefix : empresas.commonPrefixes()) {

            String empresa = empresaPrefix.prefix();
            var servidores = s3.listarComPrefixo(TRUSTED_BUCKET, empresa, true);

            for (var servidorPrefix : servidores.commonPrefixes()) {

                String servidor = servidorPrefix.prefix();
                var arquivos = s3.listarComPrefixo(TRUSTED_BUCKET, servidor, false);

                for (var arquivo : arquivos.contents()) {

                    if (!arquivo.key().contains("coleta_" + hojeStr + ".csv")) {
                        continue;
                    }

                    // LER CSV DO DIA
                    String csv = s3.baixarArquivo(TRUSTED_BUCKET, arquivo.key()).asUtf8String();

                    List<ComponenteModel> dados = ComponenteModel.parseCsv(
                            csv,
                            empresa.replace("/", ""),
                            servidor.replace(empresa, "").replace("/", "")
                    );

                    if (dados.isEmpty()) continue;

                    // Key para salvar no bucket client
                    String keySaida =
                            servidor + "componentes/dadosComponentes_" +
                                    hoje.getYear() + "-" + hoje.getMonthValue() + "-" + hoje.getDayOfMonth() + "_" +
                                    hoje.getDayOfMonth() + "-" + hoje.getMonthValue() + "-" + hoje.getYear() +
                                    ".json";

                    // Converte lista de objetos para lista de Map (S3Service espera Map)
                    List<Map<String, String>> dadosMap =
                            ComponenteModelMapper.converterLista(dados);

                    // Envia JSON para bucket client
                    s3.enviarJsonLista(CLIENT_BUCKET, keySaida, dadosMap);

                    System.out.println("ARQUIVO ATUALIZADO: " + keySaida);
                }
            }
        }
    }
}
