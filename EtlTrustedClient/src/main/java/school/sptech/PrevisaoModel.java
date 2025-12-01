package school.sptech;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class PrevisaoModel {
    private List<Double> cpu;
    private List<Double> ram;
    private List<Double> disco;
    private List<Double> latencia;
    private String empresa;
    private String servidor;
    private String dataProcessamento;
    private String periodo;

    private double crescimentoCpuPercentual;
    private String crescimentoCpuTendencia;
    private double crescimentoRamPercentual;
    private String crescimentoRamTendencia;
    private double crescimentoDiscoPercentual;
    private String crescimentoDiscoTendencia;
    private double crescimentoLatenciaPercentual;
    private String crescimentoLatenciaTendencia;

    public PrevisaoModel(List<Double> cpu, List<Double> ram, List<Double> disco,
                         List<Double> latencia, String empresa, String servidor,
                         String periodo, List<Double> historicoCpu, List<Double> historicoRam,
                         List<Double> historicoDisco, List<Double> historicoLatencia) {
        this.cpu = cpu;
        this.ram = ram;
        this.disco = disco;
        this.latencia = latencia;
        this.empresa = empresa;
        this.servidor = servidor;
        this.dataProcessamento = LocalDateTime.now().toString();
        this.periodo = periodo;

        calcularCrescimentoPorPeriodo(historicoCpu, historicoRam, historicoDisco, historicoLatencia, periodo);
    }

    public PrevisaoModel(List<Double> cpu, List<Double> ram, List<Double> disco,
                         List<Double> latencia, String empresa, String servidor, String periodo) {
        this(cpu, ram, disco, latencia, empresa, servidor, periodo,
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    private void calcularCrescimentoPorPeriodo(List<Double> historicoCpu, List<Double> historicoRam,
                                               List<Double> historicoDisco, List<Double> historicoLatencia,
                                               String periodo) {

        this.crescimentoCpuPercentual = calcularCrescimentoPeriodoEspecifico(historicoCpu, periodo);
        this.crescimentoCpuTendencia = determinarTendencia(this.crescimentoCpuPercentual);

        this.crescimentoRamPercentual = calcularCrescimentoPeriodoEspecifico(historicoRam, periodo);
        this.crescimentoRamTendencia = determinarTendencia(this.crescimentoRamPercentual);

        this.crescimentoDiscoPercentual = calcularCrescimentoPeriodoEspecifico(historicoDisco, periodo);
        this.crescimentoDiscoTendencia = determinarTendencia(this.crescimentoDiscoPercentual);

        this.crescimentoLatenciaPercentual = calcularCrescimentoPeriodoEspecifico(historicoLatencia, periodo);
        this.crescimentoLatenciaTendencia = determinarTendencia(this.crescimentoLatenciaPercentual);
    }

    private double calcularCrescimentoPeriodoEspecifico(List<Double> valores, String periodo) {
        if (valores == null || valores.isEmpty()) {
            return 0.0;
        }

        if (periodo.equals("semanal")) {
            return calcularCrescimentoSemanal(valores);
        } else if (periodo.equals("mensal")) {
            return calcularCrescimentoMensal(valores);
        } else {
            return calcularCrescimentoSimples(valores);
        }
    }

    private double calcularCrescimentoSemanal(List<Double> valores) {
        if (valores.size() >= 14) {
            int totalDias = valores.size();
            double mediaSemanaAtual = calcularMedia(valores, Math.max(0, totalDias - 7), totalDias - 1);
            double mediaSemanaAnterior = calcularMedia(valores, Math.max(0, totalDias - 14), Math.max(0, totalDias - 8));

            if (mediaSemanaAnterior > 0) {
                double crescimento = ((mediaSemanaAtual - mediaSemanaAnterior) / mediaSemanaAnterior) * 100;
                return Math.round(crescimento * 10.0) / 10.0;
            }
        } else if (valores.size() >= 7) {
            int pontoMeio = valores.size() / 2;
            double primeiraMetade = calcularMedia(valores, 0, pontoMeio - 1);
            double segundaMetade = calcularMedia(valores, pontoMeio, valores.size() - 1);

            if (primeiraMetade > 0) {
                double crescimento = ((segundaMetade - primeiraMetade) / primeiraMetade) * 100;
                return Math.round(crescimento * 10.0) / 10.0;
            }
        } else if (valores.size() >= 2) {
            return calcularCrescimentoSimples(valores);
        }

        return 0.0;
    }

    private double calcularCrescimentoMensal(List<Double> valores) {
        if (valores.size() >= 60) {
            int totalDias = valores.size();
            double mediaMesAtual = calcularMedia(valores, Math.max(0, totalDias - 30), totalDias - 1);
            double mediaMesAnterior = calcularMedia(valores, Math.max(0, totalDias - 60), Math.max(0, totalDias - 31));

            if (mediaMesAnterior > 0) {
                double crescimento = ((mediaMesAtual - mediaMesAnterior) / mediaMesAnterior) * 100;
                return Math.round(crescimento * 10.0) / 10.0;
            }
        } else if (valores.size() >= 30) {
            int pontoMeio = valores.size() / 2;
            double primeiraMetade = calcularMedia(valores, 0, pontoMeio - 1);
            double segundaMetade = calcularMedia(valores, pontoMeio, valores.size() - 1);

            if (primeiraMetade > 0) {
                double crescimento = ((segundaMetade - primeiraMetade) / primeiraMetade) * 100;
                return Math.round(crescimento * 10.0) / 10.0;
            }
        } else if (valores.size() >= 2) {
            return calcularCrescimentoSimples(valores);
        }

        return 0.0;
    }

    private double calcularMedia(List<Double> valores, int inicio, int fim) {
        if (inicio < 0 || fim >= valores.size() || inicio > fim) {
            return 0.0;
        }

        double soma = 0;
        int count = 0;

        for (int i = inicio; i <= fim; i++) {
            soma += valores.get(i);
            count++;
        }

        return count > 0 ? soma / count : 0.0;
    }

    private double calcularCrescimentoSimples(List<Double> valores) {
        if (valores.size() < 2) {
            return 0.0;
        }

        double valorAnterior = valores.get(valores.size() - 2);
        double valorAtual = valores.get(valores.size() - 1);

        if (valorAnterior > 0) {
            double crescimento = ((valorAtual - valorAnterior) / valorAnterior) * 100;
            return Math.round(crescimento * 10.0) / 10.0;
        }

        return 0.0;
    }

    private String determinarTendencia(double percentual) {
        if (percentual > 2) {
            return "crescendo";
        } else if (percentual < -2) {
            return "decrescendo";
        } else {
            return "estavel";
        }
    }

    public List<Double> getCpu() { return cpu; }
    public List<Double> getRam() { return ram; }
    public List<Double> getDisco() { return disco; }
    public List<Double> getLatencia() { return latencia; }
    public String getEmpresa() { return empresa; }
    public String getServidor() { return servidor; }
    public String getDataProcessamento() { return dataProcessamento; }
    public String getPeriodo() { return periodo; }
    public double getCrescimentoCpuPercentual() { return crescimentoCpuPercentual; }
    public String getCrescimentoCpuTendencia() { return crescimentoCpuTendencia; }
    public double getCrescimentoRamPercentual() { return crescimentoRamPercentual; }
    public String getCrescimentoRamTendencia() { return crescimentoRamTendencia; }
    public double getCrescimentoDiscoPercentual() { return crescimentoDiscoPercentual; }
    public String getCrescimentoDiscoTendencia() { return crescimentoDiscoTendencia; }
    public double getCrescimentoLatenciaPercentual() { return crescimentoLatenciaPercentual; }
    public String getCrescimentoLatenciaTendencia() { return crescimentoLatenciaTendencia; }

    public static List<PrevisaoModel> parseCsvToDados(String csvContent) {
        List<PrevisaoModel> dados = new ArrayList<>();

        if (csvContent == null || csvContent.isEmpty()) {
            return dados;
        }

        String[] linhas = csvContent.split("\n");

        int startIndex = 0;
        if (linhas.length > 0 && linhas[0].contains("id;servidor;timestamp")) {
            startIndex = 1;
        }

        for (int i = startIndex; i < linhas.length; i++) {
            String linha = linhas[i].trim();
            if (linha.isEmpty()) continue;

            try {
                PrevisaoModel dado = parseLinha(linha);
                if (dado != null) {
                    dados.add(dado);
                }
            } catch (Exception e) {
                System.out.println("Erro ao parsear linha: " + linha);
            }
        }

        return dados;
    }

    private static PrevisaoModel parseLinha(String linha) {
        String[] campos = linha.split(";");

        if (campos.length < 11) {
            return null;
        }

        try {
            if (campos[0].equals("id") || campos[2].equals("timestamp")) {
                return null;
            }

            LocalDateTime timestamp = parseTimestamp(campos[2]);

            double cpuPercent = parseDoubleComVirgula(campos[4]);
            double memoryPercent = parseDoubleComVirgula(campos[5]);
            double diskPercent = parseDoubleComVirgula(campos[8]);
            double latenciaMediaMs = parseDoubleComVirgula(campos[10]);

            List<Double> cpuList = new ArrayList<>();
            cpuList.add(cpuPercent);
            List<Double> ramList = new ArrayList<>();
            ramList.add(memoryPercent);
            List<Double> discoList = new ArrayList<>();
            discoList.add(diskPercent);
            List<Double> latenciaList = new ArrayList<>();
            latenciaList.add(latenciaMediaMs);

            return new PrevisaoModel(cpuList, ramList, discoList, latenciaList, "", "", "dado");

        } catch (Exception e) {
            return null;
        }
    }

    private static double parseDoubleComVirgula(String valor) {
        if (valor == null || valor.trim().isEmpty()) {
            return 0.0;
        }
        try {
            String valorFormatado = valor.trim().replace(",", ".");
            return Double.parseDouble(valorFormatado);
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    private static LocalDateTime parseTimestamp(String timestampStr) {
        try {
            if (timestampStr.contains(".")) {
                timestampStr = timestampStr.substring(0, timestampStr.indexOf('.'));
            }
            return LocalDateTime.parse(timestampStr, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        } catch (Exception e) {
            return LocalDateTime.now();
        }
    }
}