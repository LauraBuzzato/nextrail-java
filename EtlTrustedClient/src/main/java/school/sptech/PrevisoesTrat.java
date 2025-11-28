package school.sptech;

import software.amazon.awssdk.services.s3.model.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PrevisoesTrat {

    private static final String TRUSTED_BUCKET = "trusted-nextrail-teste";
    private static final String CLIENT_BUCKET = "client-nextrail-teste";

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

                    List<PrevisaoModel> dadosSemanais = coletarDadosHistoricos(empresa, servidor, hoje, 7);
                    if (!dadosSemanais.isEmpty()) {
                        System.out.println("Gerando previsão semanal com " + dadosSemanais.size() + " registros");
                        PrevisaoModel previsaoSemanal = gerarPrevisaoSemanal(dadosSemanais, empresa, servidor);
                        salvarPrevisaoClient(previsaoSemanal, empresa, servidor, hoje, "semanal");
                        System.out.println("Previsão semanal salva");
                    } else {
                        System.out.println("Sem dados para previsão semanal");
                    }

                    List<PrevisaoModel> dadosMensais = coletarDadosHistoricos(empresa, servidor, hoje, 30);
                    if (!dadosMensais.isEmpty()) {
                        System.out.println("Gerando previsão mensal com " + dadosMensais.size() + " registros");
                        PrevisaoModel previsaoMensal = gerarPrevisaoMensal(dadosMensais, empresa, servidor);
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

    private static List<PrevisaoModel> coletarDadosHistoricos(String empresa, String servidor,
                                                              LocalDate dataBase, int dias) {
        List<PrevisaoModel> dados = new ArrayList<>();

        try {
            String prefixo = String.format("%s/%s/", empresa, servidor);
            System.out.println("Listando arquivos em: " + prefixo);

            ListObjectsV2Response response = s3.listarComPrefixo(TRUSTED_BUCKET, prefixo, false);

            LocalDate dataLimite = dataBase.minusDays(dias - 1);
            System.out.println("Buscando arquivos entre " + dataLimite + " e " + dataBase);

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
                        dados.addAll(linhas);
                        System.out.println("Arquivo: " + key + " - Data: " + dataArquivo + " - Registros: " + linhas.size());
                    } else {
                        System.out.println("Arquivo fora do range: " + key + " - Data: " + dataArquivo);
                    }
                } catch (Exception e) {
                    System.out.println("Erro ao processar arquivo: " + key + " - " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.out.println("Erro ao coletar dados históricos: " + e.getMessage());
            e.printStackTrace();
        }

        System.out.println("Total de registros coletados para análise: " + dados.size());
        return dados;
    }

    private static PrevisaoModel gerarPrevisaoSemanal(List<PrevisaoModel> dadosHistoricos, String empresa, String servidor) {
        List<Double> mediasCpu = calcularMediasSemanais(dadosHistoricos, "cpu");
        List<Double> mediasRam = calcularMediasSemanais(dadosHistoricos, "ram");
        List<Double> mediasDisco = calcularMediasSemanais(dadosHistoricos, "disco");
        List<Double> mediasLatencia = calcularMediasSemanais(dadosHistoricos, "latencia");

        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia, empresa, servidor, "semanal");
    }


    private static PrevisaoModel gerarPrevisaoMensal(List<PrevisaoModel> dadosHistoricos, String empresa, String servidor) {
        List<Double> mediasCpu = calcularMediasMensais(dadosHistoricos, "cpu");
        List<Double> mediasRam = calcularMediasMensais(dadosHistoricos, "ram");
        List<Double> mediasDisco = calcularMediasMensais(dadosHistoricos, "disco");
        List<Double> mediasLatencia = calcularMediasMensais(dadosHistoricos, "latencia");

        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia, empresa, servidor, "mensal");
    }

    private static List<Double> calcularMediasSemanais(List<PrevisaoModel> dados, String componente) {
        List<Double> medias = new ArrayList<>();

        if (dados.isEmpty()) {
            return medias;
        }

        int registrosPorSemana = 7;
        int numSemanas = Math.max(1, (dados.size() + registrosPorSemana - 1) / registrosPorSemana);

        numSemanas = Math.min(4, numSemanas);

        for (int i = 0; i < numSemanas; i++) {
            int inicio = i * registrosPorSemana;
            int fim = Math.min((i + 1) * registrosPorSemana, dados.size());


            if (inicio >= dados.size()) {
                break;
            }

            double soma = 0;
            int count = 0;

            for (int j = inicio; j < fim; j++) {
                switch (componente) {
                    case "cpu": soma += dados.get(j).getCpu().get(0); break;
                    case "ram": soma += dados.get(j).getRam().get(0); break;
                    case "disco": soma += dados.get(j).getDisco().get(0); break;
                    case "latencia": soma += dados.get(j).getLatencia().get(0); break;
                }
                count++;
            }

            if (count > 0) {
                medias.add(soma / count);
            }
        }

        return medias;
    }

    private static List<Double> calcularMediasMensais(List<PrevisaoModel> dados, String componente) {
        List<Double> medias = new ArrayList<>();

        if (dados.isEmpty()) {
            return medias;
        }

        int registrosPorMes = 30;
        int numMeses = Math.max(1, (dados.size() + registrosPorMes - 1) / registrosPorMes);

        numMeses = Math.min(3, numMeses);

        for (int i = 0; i < numMeses; i++) {
            int inicio = i * registrosPorMes;
            int fim = Math.min((i + 1) * registrosPorMes, dados.size());

            if (inicio >= dados.size()) {
                break;
            }

            double soma = 0;
            int count = 0;

            for (int j = inicio; j < fim; j++) {
                switch (componente) {
                    case "cpu": soma += dados.get(j).getCpu().get(0); break;
                    case "ram": soma += dados.get(j).getRam().get(0); break;
                    case "disco": soma += dados.get(j).getDisco().get(0); break;
                    case "latencia": soma += dados.get(j).getLatencia().get(0); break;
                }
                count++;
            }

            if (count > 0) {
                medias.add(soma / count);
            }
        }

        return medias;
    }

    private static List<Double> calcularPrevisaoComHistorico(List<Double> historico) {
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

        int n = historico.size();
        double sumX = 0, sumY = 0, sumXY = 0, sumX2 = 0;

        for (int i = 0; i < n; i++) {
            sumX += i;
            sumY += historico.get(i);
            sumXY += i * historico.get(i);
            sumX2 += i * i;
        }

        double slope = (n * sumXY - sumX * sumY) / (n * sumX2 - sumX * sumX);
        double intercept = (sumY - slope * sumX) / n;

        for (int i = n; i < n + 2; i++) {
            double previsao = slope * i + intercept;
            previsao = Math.max(0, Math.min(100, previsao));
            resultado.add(Math.round(previsao * 10.0) / 10.0);
        }

        return resultado;
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

        System.out.println("      Salvo em: " + key);
    }

    private static String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }
}