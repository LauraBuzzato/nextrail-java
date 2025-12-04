package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class ARIMAImplementation {

    public static double[] preverComSARIMAAdaptado(List<Double> mediasHistoricas, String periodo) {
        if (mediasHistoricas == null || mediasHistoricas.size() < 3) {
            return new double[]{0.0, 0.0, 0.0, 0.0};
        }

        try {
            double[] dados = new double[mediasHistoricas.size()];
            for (int i = 0; i < mediasHistoricas.size(); i++) {
                dados[i] = mediasHistoricas.get(i);
            }

            int tamanho = dados.length;
            double mediaAnterior = dados[tamanho - 2];
            double mediaAtual = dados[tamanho - 1];

            int periodoSazonal = periodo.equals("semanal") ? 4 : 12;

            List<Double> previsoesList;
            if (mediasHistoricas.size() >= periodoSazonal * 2) {
                previsoesList = preverSARIMA(dados, 2, periodoSazonal);
            } else {
                previsoesList = preverARIMA(dados, 2);
            }

            double previsaoProxima = previsoesList.size() > 0 ? previsoesList.get(0) : mediaAtual;
            double previsaoSubsequente = previsoesList.size() > 1 ? previsoesList.get(1) : previsaoProxima;

            return new double[]{
                    Math.round(mediaAnterior * 10.0) / 10.0,
                    Math.round(mediaAtual * 10.0) / 10.0,
                    Math.round(previsaoProxima * 10.0) / 10.0,
                    Math.round(previsaoSubsequente * 10.0) / 10.0
            };

        } catch (Exception e) {
            int tamanho = mediasHistoricas.size();
            double mediaAnterior = tamanho >= 2 ? mediasHistoricas.get(tamanho - 2) : 0.0;
            double mediaAtual = tamanho >= 1 ? mediasHistoricas.get(tamanho - 1) : 0.0;

            return new double[]{
                    Math.round(mediaAnterior * 10.0) / 10.0,
                    Math.round(mediaAtual * 10.0) / 10.0,
                    Math.round(mediaAtual * 10.0) / 10.0,
                    Math.round(mediaAtual * 10.0) / 10.0
            };
        }
    }

    private static List<Double> preverARIMA(double[] dados, int previsoes) {
        List<Double> previsoesList = new ArrayList<>();
        int n = dados.length;

        if (n < 2) {
            for (int i = 0; i < previsoes; i++) {
                previsoesList.add(n > 0 ? dados[n - 1] : 0.0);
            }
            return previsoesList;
        }

        double[] diferencas = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            diferencas[i] = dados[i + 1] - dados[i];
        }

        double mediaDiferencas = calcularMedia(diferencas);
        double ultimoValor = dados[n - 1];

        for (int i = 0; i < previsoes; i++) {
            double previsao = ultimoValor + mediaDiferencas * (i + 1);
            previsao = Math.max(0, Math.min(100, previsao));
            previsoesList.add(Math.round(previsao * 10.0) / 10.0);
        }

        return previsoesList;
    }

    private static List<Double> preverSARIMA(double[] dados, int previsoes, int periodoSazonal) {
        List<Double> previsoesList = new ArrayList<>();
        int n = dados.length;

        double[] componentesSazonais = extrairSazonalidade(dados, periodoSazonal);
        double[] dadosDessazonalizados = dessazonalizar(dados, componentesSazonais, periodoSazonal);

        double tendencia = calcularTendencia(dadosDessazonalizados);
        double nivel = dadosDessazonalizados[n - 1];

        for (int i = 0; i < previsoes; i++) {
            double previsaoDessazonalizada = nivel + tendencia * (i + 1);

            int indiceSazonal = (n + i) % periodoSazonal;
            double componenteSazonal = componentesSazonais[indiceSazonal];

            double previsaoFinal = previsaoDessazonalizada + componenteSazonal;
            previsaoFinal = Math.max(0, Math.min(100, previsaoFinal));

            previsoesList.add(Math.round(previsaoFinal * 10.0) / 10.0);
        }

        return previsoesList;
    }

    private static double[] extrairSazonalidade(double[] dados, int periodo) {
        double[] sazonalidade = new double[periodo];
        int ciclosCompletos = dados.length / periodo;

        if (ciclosCompletos < 1) {
            return sazonalidade;
        }

        for (int i = 0; i < periodo; i++) {
            double soma = 0;
            int contador = 0;

            for (int ciclo = 0; ciclo < ciclosCompletos; ciclo++) {
                int indice = ciclo * periodo + i;
                if (indice < dados.length) {
                    soma += dados[indice];
                    contador++;
                }
            }

            sazonalidade[i] = contador > 0 ? soma / contador : 0;
        }

        double mediaSazonal = calcularMedia(sazonalidade);
        for (int i = 0; i < periodo; i++) {
            sazonalidade[i] -= mediaSazonal;
        }

        return sazonalidade;
    }

    private static double[] dessazonalizar(double[] dados, double[] sazonalidade, int periodo) {
        double[] dessazonalizados = new double[dados.length];

        for (int i = 0; i < dados.length; i++) {
            int indiceSazonal = i % periodo;
            dessazonalizados[i] = dados[i] - sazonalidade[indiceSazonal];
        }

        return dessazonalizados;
    }

    private static double calcularTendencia(double[] dados) {
        int n = dados.length;
        if (n < 2) return 0;

        double[] diferencas = new double[n - 1];
        for (int i = 0; i < n - 1; i++) {
            diferencas[i] = dados[i + 1] - dados[i];
        }

        return calcularMedia(diferencas);
    }

    private static double calcularMediaMovel(double[] dados, int janela) {
        int n = Math.min(janela, dados.length);
        double soma = 0;

        for (int i = dados.length - n; i < dados.length; i++) {
            soma += dados[i];
        }

        return soma / n;
    }

    private static double calcularMedia(double[] valores) {
        if (valores.length == 0) return 0;
        double soma = 0;
        for (double v : valores) soma += v;
        return soma / valores.length;
    }
}