package school.sptech;

import software.amazon.awssdk.services.s3.model.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class PrevisoesTrat {

    private static final String TRUSTED_BUCKET = "bucket-nextrail-trusted";
    private static final String CLIENT_BUCKET = "bucket-nextrail-client";

    private static final S3Service s3 = new S3Service();

    public static void processarPrevisoes() throws Exception {
        LocalDate hoje = LocalDate.now();

        try {
            ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

            for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
                String empresa = empresaPrefix.prefix().replace("/", "");

                ListObjectsV2Response servidores = s3.listarComPrefixo(TRUSTED_BUCKET, empresa + "/", true);

                for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {
                    String servidorCompleto = servidorPrefix.prefix();
                    String servidor = servidorCompleto.replace(empresa + "/", "").replace("/", "");

                    List<PrevisaoModel> dadosHistoricos = coletarDadosHistoricos(empresa, servidor, hoje);

                    if (!dadosHistoricos.isEmpty()) {
                        PrevisaoModel previsaoSemanal = gerarPrevisaoSemanal(dadosHistoricos, empresa, servidor);
                        salvarPrevisaoClient(previsaoSemanal, empresa, servidor, hoje, "semanal");

                        PrevisaoModel previsaoMensal = gerarPrevisaoMensal(dadosHistoricos, empresa, servidor);
                        salvarPrevisaoClient(previsaoMensal, empresa, servidor, hoje, "mensal");
                    }
                }
            }

        } catch (Exception e) {
            System.out.println("Erro no processamento de previsões: " + e.getMessage());
        }
    }

    private static List<PrevisaoModel> coletarDadosHistoricos(String empresa, String servidor, LocalDate dataBase) {
        List<PrevisaoModel> dados = new ArrayList<>();

        try {
            for (int i = 30; i >= 1; i--) {
                LocalDate data = dataBase.minusDays(i);
                String arquivoKey = String.format("%s/%s/coleta_%s.csv", empresa, servidor, data.toString());

                try {
                    String csvContent = s3.baixarArquivo(TRUSTED_BUCKET, arquivoKey).asUtf8String();
                    List<PrevisaoModel> linhas = PrevisaoModel.parseCsvToDados(csvContent);
                    dados.addAll(linhas);
                } catch (Exception e) {
                    continue;
                }
            }
        } catch (Exception e) {
            System.out.println("Erro ao coletar dados históricos: " + e.getMessage());
        }

        return dados;
    }

    private static PrevisaoModel gerarPrevisaoSemanal(List<PrevisaoModel> dadosHistoricos, String empresa, String servidor) {
        List<Double> mediasCpu = calcularMediasSemanais(dadosHistoricos, "cpu");
        List<Double> mediasRam = calcularMediasSemanais(dadosHistoricos, "ram");
        List<Double> mediasDisco = calcularMediasSemanais(dadosHistoricos, "disco");
        List<Double> mediasLatencia = calcularMediasSemanais(dadosHistoricos, "latencia");

        List<Double> previsaoCpu = calcularPrevisaoTendencia(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoTendencia(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoTendencia(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoTendencia(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia, empresa, servidor, "semanal");
    }

    private static PrevisaoModel gerarPrevisaoMensal(List<PrevisaoModel> dadosHistoricos, String empresa, String servidor) {
        List<Double> mediasCpu = calcularMediasMensais(dadosHistoricos, "cpu");
        List<Double> mediasRam = calcularMediasMensais(dadosHistoricos, "ram");
        List<Double> mediasDisco = calcularMediasMensais(dadosHistoricos, "disco");
        List<Double> mediasLatencia = calcularMediasMensais(dadosHistoricos, "latencia");

        List<Double> previsaoCpu = calcularPrevisaoTendencia(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoTendencia(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoTendencia(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoTendencia(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia, empresa, servidor, "mensal");
    }

    private static List<Double> calcularMediasSemanais(List<PrevisaoModel> dados, String componente) {
        List<Double> medias = new ArrayList<>();

        int semanas = Math.min(4, dados.size() / 7);

        for (int i = 0; i < semanas; i++) {
            int inicio = i * 7;
            int fim = Math.min((i + 1) * 7, dados.size());

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

        int meses = Math.min(3, dados.size() / 30);

        for (int i = 0; i < meses; i++) {
            int inicio = i * 30;
            int fim = Math.min((i + 1) * 30, dados.size());

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

    private static List<Double> calcularPrevisaoTendencia(List<Double> historico) {
        List<Double> previsoes = new ArrayList<>();

        if (historico.size() < 2) {
            double ultimoValor = historico.isEmpty() ? 50.0 : historico.get(historico.size() - 1);
            for (int i = 0; i < 4; i++) {
                previsoes.add(ultimoValor);
            }
            return previsoes;
        }

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

        for (int i = n; i < n + 4; i++) {
            double previsao = slope * i + intercept;
            previsao = Math.max(0, Math.min(100, previsao));
            previsoes.add(Math.round(previsao * 10.0) / 10.0);
        }

        return previsoes;
    }

    private static void salvarPrevisaoClient(PrevisaoModel previsao, String empresa, String servidor, LocalDate data, String tipoPeriodo) throws Exception {
        String empresaFormatada = formatarNome(empresa);
        String servidorFormatado = formatarNome(servidor);

        String key = String.format("%s/%s/previsoes/dadosPrev_%s_%s.json",
                empresaFormatada, servidorFormatado,
                data.format(DateTimeFormatter.ofPattern("dd-MM-yyyy")),
                tipoPeriodo);

        s3.enviarJsonObject(CLIENT_BUCKET, key, previsao);
    }

    private static String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }
}