package school.sptech;

import java.util.ArrayList;
import java.util.List;

public class ARIMAImplementation {

    public static List<Double> preverComSARIMA(List<Double> historico, int previsoes, String periodo) {
        List<Double> resultado = new ArrayList<>();

        if (historico == null || historico.size() < 8) {
            return calcularPrevisaoLinear(historico, previsoes);
        }

        try {
            double[] dados = new double[historico.size()];
            for (int i = 0; i < historico.size(); i++) {
                dados[i] = historico.get(i);
            }

            int periodoSazonal = periodo.equals("semanal") ? 7 : 30;

            if (historico.size() >= periodoSazonal * 2) {
                return preverSARIMA(dados, previsoes, periodoSazonal);
            } else {
                return preverARIMA(dados, previsoes);
            }

        } catch (Exception e) {
            return calcularPrevisaoLinear(historico, previsoes);
        }
    }

    private static List<Double> preverARIMA(double[] dados, int previsoes) {
        List<Double> previsoesList = new ArrayList<>();
        int n = dados.length;

        double ultimoValor = dados[n - 1];
        double penultimoValor = dados[n - 2];

        double tendencia = ultimoValor - penultimoValor;
        double mediaMovel = calcularMediaMovel(dados, 3);

        for (int i = 0; i < previsoes; i++) {
            double previsao = ultimoValor + tendencia * (i + 1) * 0.8;
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

        double primeiro = dados[0];
        double ultimo = dados[n - 1];

        return (ultimo - primeiro) / (n - 1);
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
        double soma = 0;
        for (double v : valores) soma += v;
        return soma / valores.length;
    }

    private static List<Double> calcularPrevisaoLinear(List<Double> historico, int previsoes) {
        List<Double> resultado = new ArrayList<>();

        if (historico == null || historico.isEmpty()) {
            for (int i = 0; i < previsoes; i++) {
                resultado.add(0.0);
            }
            return resultado;
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

        for (int i = n; i < n + previsoes; i++) {
            double previsao = slope * i + intercept;
            previsao = Math.max(0, Math.min(100, previsao));
            resultado.add(Math.round(previsao * 10.0) / 10.0);
        }

        return resultado;
    }
}