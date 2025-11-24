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

    public PrevisaoModel(List<Double> cpu, List<Double> ram, List<Double> disco,
                         List<Double> latencia, String empresa, String servidor, String periodo) {
        this.cpu = cpu;
        this.ram = ram;
        this.disco = disco;
        this.latencia = latencia;
        this.empresa = empresa;
        this.servidor = servidor;
        this.dataProcessamento = LocalDateTime.now().toString();
        this.periodo = periodo;
    }

    public List<Double> getCpu() { return cpu; }
    public List<Double> getRam() { return ram; }
    public List<Double> getDisco() { return disco; }
    public List<Double> getLatencia() { return latencia; }
    public String getEmpresa() { return empresa; }
    public String getServidor() { return servidor; }
    public String getDataProcessamento() { return dataProcessamento; }
    public String getPeriodo() { return periodo; }

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
            LocalDateTime timestamp = parseTimestamp(campos[2]);
            double cpuPercent = Double.parseDouble(campos[4]);
            double memoryPercent = Double.parseDouble(campos[5]);
            double diskPercent = Double.parseDouble(campos[8]);
            double latenciaMediaMs = Double.parseDouble(campos[10]);

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