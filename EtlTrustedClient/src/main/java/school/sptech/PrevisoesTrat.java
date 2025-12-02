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
        List<Double> todosValoresCpu = new ArrayList<>();
        List<Double> todosValoresRam = new ArrayList<>();
        List<Double> todosValoresDisco = new ArrayList<>();
        List<Double> todosValoresLatencia = new ArrayList<>();

        for (PrevisaoModel dado : dadosHistoricos) {
            todosValoresCpu.add(dado.getCpu().get(0));
            todosValoresRam.add(dado.getRam().get(0));
            todosValoresDisco.add(dado.getDisco().get(0));
            todosValoresLatencia.add(dado.getLatencia().get(0));
        }

        List<Double> mediasCpu = calcularMediasSemanais(dadosHistoricos, "cpu", todosValoresCpu);
        List<Double> mediasRam = calcularMediasSemanais(dadosHistoricos, "ram", todosValoresRam);
        List<Double> mediasDisco = calcularMediasSemanais(dadosHistoricos, "disco", todosValoresDisco);
        List<Double> mediasLatencia = calcularMediasSemanais(dadosHistoricos, "latencia", todosValoresLatencia);

        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia,
                empresa, servidor, "semanal", todosValoresCpu, todosValoresRam, todosValoresDisco, todosValoresLatencia);
    }

    private static PrevisaoModel gerarPrevisaoMensal(List<PrevisaoModel> dadosHistoricos, String empresa, String servidor) {
        List<Double> todosValoresCpu = new ArrayList<>();
        List<Double> todosValoresRam = new ArrayList<>();
        List<Double> todosValoresDisco = new ArrayList<>();
        List<Double> todosValoresLatencia = new ArrayList<>();

        for (PrevisaoModel dado : dadosHistoricos) {
            todosValoresCpu.add(dado.getCpu().get(0));
            todosValoresRam.add(dado.getRam().get(0));
            todosValoresDisco.add(dado.getDisco().get(0));
            todosValoresLatencia.add(dado.getLatencia().get(0));
        }

        List<Double> mediasCpu = calcularMediasMensais(dadosHistoricos, "cpu", todosValoresCpu);
        List<Double> mediasRam = calcularMediasMensais(dadosHistoricos, "ram", todosValoresRam);
        List<Double> mediasDisco = calcularMediasMensais(dadosHistoricos, "disco", todosValoresDisco);
        List<Double> mediasLatencia = calcularMediasMensais(dadosHistoricos, "latencia", todosValoresLatencia);

        List<Double> previsaoCpu = calcularPrevisaoComHistorico(mediasCpu);
        List<Double> previsaoRam = calcularPrevisaoComHistorico(mediasRam);
        List<Double> previsaoDisco = calcularPrevisaoComHistorico(mediasDisco);
        List<Double> previsaoLatencia = calcularPrevisaoComHistorico(mediasLatencia);

        return new PrevisaoModel(previsaoCpu, previsaoRam, previsaoDisco, previsaoLatencia,
                empresa, servidor, "mensal", todosValoresCpu, todosValoresRam, todosValoresDisco, todosValoresLatencia);
    }

    private static List<Double> calcularMediasSemanais(List<PrevisaoModel> dados, String componente, List<Double> todosValores) {
        List<Double> medias = new ArrayList<>();

        if (dados.isEmpty()) {
            return medias;
        }

        if (todosValores.size() >= 7) {
            int numSemanas = Math.min(4, (todosValores.size() + 6) / 7);

            for (int semana = 0; semana < numSemanas; semana++) {
                int inicio = semana * 7;
                int fim = Math.min((semana + 1) * 7, todosValores.size());

                double soma = 0;
                int count = 0;

                for (int i = inicio; i < fim; i++) {
                    soma += todosValores.get(i);
                    count++;
                }

                if (count > 0) {
                    medias.add(soma / count);
                }
            }
        } else {
            medias.addAll(todosValores);
        }

        return medias;
    }

    private static List<Double> calcularMediasMensais(List<PrevisaoModel> dados, String componente, List<Double> todosValores) {
        List<Double> medias = new ArrayList<>();

        if (dados.isEmpty()) {
            return medias;
        }

        if (todosValores.size() >= 14) {
            if (todosValores.size() >= 30) {
                int numMeses = Math.min(3, (todosValores.size() + 29) / 30);

                for (int mes = 0; mes < numMeses; mes++) {
                    int inicio = mes * 30;
                    int fim = Math.min((mes + 1) * 30, todosValores.size());

                    double soma = 0;
                    int count = 0;

                    for (int i = inicio; i < fim; i++) {
                        soma += todosValores.get(i);
                        count++;
                    }

                    if (count > 0) {
                        medias.add(soma / count);
                    }
                }
            } else {
                int numQuinzenas = Math.min(4, (todosValores.size() + 14) / 15);

                for (int quinzena = 0; quinzena < numQuinzenas; quinzena++) {
                    int inicio = quinzena * 15;
                    int fim = Math.min((quinzena + 1) * 15, todosValores.size());

                    double soma = 0;
                    int count = 0;

                    for (int i = inicio; i < fim; i++) {
                        soma += todosValores.get(i);
                        count++;
                    }

                    if (count > 0) {
                        medias.add(soma / count);
                    }
                }
            }
        } else {
            medias.addAll(todosValores);
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

        System.out.println("Salvo em: " + key);
    }

    private static String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }
}