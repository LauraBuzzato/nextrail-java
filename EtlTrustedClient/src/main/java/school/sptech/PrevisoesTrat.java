package school.sptech;

import software.amazon.awssdk.services.s3.model.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PrevisoesTrat {

    private static final String TRUSTED_BUCKET = "nextrail-trusted-log";
    private static final String CLIENT_BUCKET = "nextrail-client-log";

    private static final S3Service s3 = new S3Service();

    public static void processarPrevisoes() throws Exception {
        LocalDate hoje = LocalDate.now();
        try {
            ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

            for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
                String empresa = empresaPrefix.prefix().replace("/", "");
                System.out.println("Processando empresa: " + empresa);

                ListObjectsV2Response servidores = s3.listarComPrefixo(TRUSTED_BUCKET, empresa + "/", true);

                for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {
                    String servidorCompleto = servidorPrefix.prefix();
                    String servidor = servidorCompleto.replace(empresa + "/", "").replace("/", "");
                    System.out.println("Processando servidor: " + servidor);

                    List<Double> mediasDiariasCpu = coletarMediasDiarias(empresa, servidor, hoje, 7, "cpu");
                    List<Double> mediasDiariasRam = coletarMediasDiarias(empresa, servidor, hoje, 7, "ram");
                    List<Double> mediasDiariasDisco = coletarMediasDiarias(empresa, servidor, hoje, 7, "disco");
                    List<Double> mediasDiariasLatencia = coletarMediasDiarias(empresa, servidor, hoje, 7, "latencia");

                    if (!mediasDiariasCpu.isEmpty()) {
                        System.out.println("Gerando previsão semanal com " + mediasDiariasCpu.size() + " médias diárias");
                        PrevisaoModel previsaoSemanal = gerarPrevisaoSemanal(
                                mediasDiariasCpu, mediasDiariasRam, mediasDiariasDisco, mediasDiariasLatencia,
                                empresa, servidor);
                        salvarPrevisaoClient(previsaoSemanal, empresa, servidor, hoje, "semanal");
                        System.out.println("Previsão semanal salva");
                    } else {
                        System.out.println("Sem dados para previsão semanal");
                    }

                    List<Double> mediasMensaisCpu = coletarMediasDiarias(empresa, servidor, hoje, 30, "cpu");
                    List<Double> mediasMensaisRam = coletarMediasDiarias(empresa, servidor, hoje, 30, "ram");
                    List<Double> mediasMensaisDisco = coletarMediasDiarias(empresa, servidor, hoje, 30, "disco");
                    List<Double> mediasMensaisLatencia = coletarMediasDiarias(empresa, servidor, hoje, 30, "latencia");

                    if (!mediasMensaisCpu.isEmpty()) {
                        System.out.println("Gerando previsão mensal com " + mediasMensaisCpu.size() + " médias diárias");
                        PrevisaoModel previsaoMensal = gerarPrevisaoMensal(
                                mediasMensaisCpu, mediasMensaisRam, mediasMensaisDisco, mediasMensaisLatencia,
                                empresa, servidor);
                        salvarPrevisaoClient(previsaoMensal, empresa, servidor, hoje, "mensal");
                        System.out.println("Previsão mensal salva");
                    } else {
                        System.out.println("Sem dados para previsão mensal");
                    }
                }
            }

            System.out.println("\nProcessamento de previsões concluído!");

        } catch (Exception e) {
            System.out.println("Erro no processamento de previsões: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static List<Double> coletarMediasDiarias(String empresa, String servidor,
                                                     LocalDate dataBase, int dias, String componente) {
        List<Double> mediasDiarias = new ArrayList<>();
        Map<LocalDate, List<Double>> valoresPorData = new TreeMap<>();

        try {
            String prefixo = String.format("%s/%s/", empresa, servidor);
            ListObjectsV2Response response = s3.listarComPrefixo(TRUSTED_BUCKET, prefixo, false);

            LocalDate dataLimite = dataBase.minusDays(dias - 1);

            for (S3Object objeto : response.contents()) {
                String key = objeto.key();

                if (!key.endsWith(".csv") || !key.contains("coleta_")) {
                    continue;
                }

                try {
                    String dataStr = key.substring(key.lastIndexOf("coleta_") + 7, key.lastIndexOf(".csv"));
                    LocalDate dataArquivo = LocalDate.parse(dataStr);

                    if (!dataArquivo.isBefore(dataLimite) && !dataArquivo.isAfter(dataBase)) {
                        String csvContent = s3.baixarArquivo(TRUSTED_BUCKET, key).asUtf8String();
                        List<PrevisaoModel> linhas = PrevisaoModel.parseCsvToDados(csvContent);

                        List<Double> valoresDoDia = new ArrayList<>();
                        for (PrevisaoModel linha : linhas) {
                            double valor = 0.0;
                            switch (componente.toLowerCase()) {
                                case "cpu":
                                    valor = linha.getCpu().get(0);
                                    break;
                                case "ram":
                                    valor = linha.getRam().get(0);
                                    break;
                                case "disco":
                                    valor = linha.getDisco().get(0);
                                    break;
                                case "latencia":
                                    valor = linha.getLatencia().get(0);
                                    break;
                            }
                            if (valor > 0) {
                                valoresDoDia.add(valor);
                            }
                        }

                        if (!valoresDoDia.isEmpty()) {
                            valoresPorData.put(dataArquivo, valoresDoDia);
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Erro ao processar arquivo: " + key);
                }
            }

            for (LocalDate data : valoresPorData.keySet()) {
                List<Double> valoresDoDia = valoresPorData.get(data);
                double mediaDoDia = calcularMedia(valoresDoDia);
                mediasDiarias.add(mediaDoDia);
            }

        } catch (Exception e) {
            System.out.println("Erro ao coletar médias diárias: " + e.getMessage());
        }

        System.out.println("Médias diárias coletadas para " + componente + ": " + mediasDiarias.size());
        return mediasDiarias;
    }

    private static PrevisaoModel gerarPrevisaoSemanal(List<Double> mediasCpu, List<Double> mediasRam,
                                                      List<Double> mediasDisco, List<Double> mediasLatencia,
                                                      String empresa, String servidor) {
        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu, "semanal");
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam, "semanal");
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco, "semanal");
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia, "semanal");

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia,
                empresa, servidor, "semanal", mediasCpu, mediasRam, mediasDisco, mediasLatencia);
    }

    private static PrevisaoModel gerarPrevisaoMensal(List<Double> mediasCpu, List<Double> mediasRam,
                                                     List<Double> mediasDisco, List<Double> mediasLatencia,
                                                     String empresa, String servidor) {
        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu, "mensal");
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam, "mensal");
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco, "mensal");
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia, "mensal");

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia,
                empresa, servidor, "mensal", mediasCpu, mediasRam, mediasDisco, mediasLatencia);
    }

    private static List<Double> calcularPrevisaoComHistorico(List<Double> historico, String periodo) {
        List<Double> resultado = new ArrayList<>();

        if (historico.isEmpty()) {
            return resultado;
        }

        if (historico.size() == 1) {
            double valor = historico.get(0);
            resultado.add(valor);
            resultado.add(valor);
            resultado.add(valor);
            resultado.add(valor);
            return resultado;
        }

        int ultimoIndex = historico.size() - 1;
        resultado.add(historico.get(ultimoIndex - 1));
        resultado.add(historico.get(ultimoIndex));

        double[] previsoesArray = ARIMAImplementation.preverComSARIMAAdaptado(historico, periodo);

        if (previsoesArray.length >= 4) {
            resultado.add(previsoesArray[2]);
            resultado.add(previsoesArray[3]);
        } else {
            resultado.add(historico.get(ultimoIndex));
            resultado.add(historico.get(ultimoIndex));
        }

        return resultado;
    }

    private static double calcularMedia(List<Double> valores) {
        if (valores == null || valores.isEmpty()) return 0.0;
        double soma = 0;
        for (Double valor : valores) {
            soma += valor;
        }
        return soma / valores.size();
    }

    private static void salvarPrevisaoClient(PrevisaoModel previsao, String empresa, String servidor,
                                             LocalDate data, String tipoPeriodo) throws Exception {
        String empresaFormatada = formatarNome(empresa);
        String servidorFormatado = formatarNome(servidor);

        String key = String.format("%s/%s/previsoes/dadosPrev_%s_%s.json",
                empresaFormatada,
                servidorFormatado,
                data.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")),
                tipoPeriodo);
        s3.enviarJsonObject(CLIENT_BUCKET, key, previsao);

        System.out.println("Salvo em: " + key);
    }

    private static String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }
}