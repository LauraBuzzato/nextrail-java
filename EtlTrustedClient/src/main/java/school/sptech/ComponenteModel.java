package school.sptech;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ComponenteModel {

    private String cpu;
    private String ram;
    private String disco;
    private String empresa;
    private String servidor;
    private String dataProcessamento;

    public ComponenteModel(String cpu, String ram, String disco,
                           String empresa, String servidor, String dataProcessamento) {
        this.cpu = cpu;
        this.ram = ram;
        this.disco = disco;
        this.empresa = empresa;
        this.servidor = servidor;
        this.dataProcessamento = dataProcessamento;
    }

    public String getCpu() { return cpu; }
    public String getRam() { return ram; }
    public String getDisco() { return disco; }
    public String getEmpresa() { return empresa; }
    public String getServidor() { return servidor; }
    public String getDataProcessamento() { return dataProcessamento; }


    // -------------------------------------------
    //       PARSE COMPLETO DO CSV (trusted)
    // -------------------------------------------
    public static List<ComponenteModel> parseCsv(String csvContent,
                                                 String empresa, String servidor) {

        List<ComponenteModel> dados = new ArrayList<>();

        if (csvContent == null || csvContent.isEmpty()) {
            return dados;
        }

        String[] linhas = csvContent.split("\n");

        int startIndex = 0;
        if (linhas[0].contains("id;servidor;timestamp")) {
            startIndex = 1;
        }

        for (int i = startIndex; i < linhas.length; i++) {

            String linha = linhas[i].trim();
            if (linha.isEmpty()) continue;

            try {
                ComponenteModel item = parseLinha(linha, empresa, servidor);

                if (item != null) dados.add(item);

            } catch (Exception e) {
                System.out.println("Erro ao parsear linha: " + linha);
            }
        }

        return dados;
    }


    private static ComponenteModel parseLinha(String linha,
                                              String empresa, String servidor) {

        String[] campos = linha.split(";");

        if (campos.length < 9) return null;

        try {
            double cpuRaw = Double.parseDouble(campos[4]);
            double ramRaw = Double.parseDouble(campos[5]);
            double discoRaw = Double.parseDouble(campos[8]);

            String cpuFmt = String.format("%.2f", cpuRaw);
            String ramFmt = String.format("%.2f", ramRaw);
            String discoFmt = String.format("%.2f", discoRaw);

            String dataProcessamento =
                    LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

            return new ComponenteModel(cpuFmt, ramFmt, discoFmt,
                    empresa, servidor, dataProcessamento);

        } catch (Exception e) {
            return null;
        }
    }
}
