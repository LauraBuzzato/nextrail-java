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

        calcularCrescimento(historicoCpu, historicoRam, historicoDisco, historicoLatencia);
    }

    public PrevisaoModel(List<Double> cpu, List<Double> ram, List<Double> disco,
                         List<Double> latencia, String empresa, String servidor, String periodo) {
        this(cpu, ram, disco, latencia, empresa, servidor, periodo,
                new ArrayList<>(), new ArrayList<>(), new ArrayList<>(), new ArrayList<>());
    }

    private void calcularCrescimento(List<Double> historicoCpu, List<Double> historicoRam,
                                     List<Double> historicoDisco, List<Double> historicoLatencia) {

        this.crescimentoCpuPercentual = calcularCrescimentoPercentual(historicoCpu);
        this.crescimentoCpuTendencia = determinarTendencia(this.crescimentoCpuPercentual);

        this.crescimentoRamPercentual = calcularCrescimentoPercentual(historicoRam);
        this.crescimentoRamTendencia = determinarTendencia(this.crescimentoRamPercentual);

        this.crescimentoDiscoPercentual = calcularCrescimentoPercentual(historicoDisco);
        this.crescimentoDiscoTendencia = determinarTendencia(this.crescimentoDiscoPercentual);

        this.crescimentoLatenciaPercentual = calcularCrescimentoPercentual(historicoLatencia);
        this.crescimentoLatenciaTendencia = determinarTendencia(this.crescimentoLatenciaPercentual);
    }

    private double calcularCrescimentoPercentual(List<Double> historico) {
        if (historico.size() < 2) {
            return 0.0;
        }

        double valorAnterior = historico.get(historico.size() - 2);
        double valorAtual = historico.get(historico.size() - 1);

        if (valorAnterior > 0) {
            return Math.round(((valorAtual - valorAnterior) / valorAnterior) * 100 * 10.0) / 10.0;
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