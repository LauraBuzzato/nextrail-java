package school.sptech;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ComponenteModel {

    private Double cpu;
    private Double ram;
    private Double disco;

    private String empresa;
    private String servidor;
    private String timestampCaptura;

    public ComponenteModel(Double cpu, Double ram, Double disco,
                           String empresa, String servidor,
                           String timestampCaptura) {
        this.cpu = cpu;
        this.ram = ram;
        this.disco = disco;
        this.empresa = empresa;
        this.servidor = servidor;
        this.timestampCaptura = timestampCaptura;
    }

    public Double getCpu() { return cpu; }
    public Double getRam() { return ram; }
    public Double getDisco() { return disco; }

    public String getEmpresa() { return empresa; }
    public String getServidor() { return servidor; }
    public String getTimestampCaptura() { return timestampCaptura; }

    /* ------------------------------------------------------------
       PARSE CSV → LISTA DE ComponenteModel (igual ao PrevisaoModel)
       ------------------------------------------------------------ */

    public static List<ComponenteModel> parseCsv(String csvContent, String empresa, String servidor) {

        List<ComponenteModel> dados = new ArrayList<>();

        if (csvContent == null || csvContent.isEmpty()) {
            return dados;
        }

        String[] linhas = csvContent.split("\n");

        // Pular cabeçalho se existir (igual ao PrevisaoModel)
        int startIndex = 0;
        if (linhas.length > 0 && linhas[0].contains("id;servidor;timestamp")) {
            startIndex = 1;
        }

        for (int i = startIndex; i < linhas.length; i++) {

            String linha = linhas[i].trim();
            if (linha.isEmpty()) continue;

            try {
                ComponenteModel dado = parseLinha(linha, empresa, servidor);
                if (dado != null) {
                    dados.add(dado);
                }
            } catch (Exception e) {
                System.out.println("Erro ao ler linha do CSV: " + linha);
            }

        }

        return dados;
    }

    private static ComponenteModel parseLinha(String linha, String empresa, String servidor) {

        String[] campos = linha.split(";");

        /*
         Estrutura do CSV original:

         0  id
         1  servidor
         2  timestamp
         3  cpu_cores
         4  cpu_percent
         5  ram_percent
         6  ram_usada
         7  ram_total
         8  disco_percent
         9  disco_usado
         10 disco_total
         ...
        */

        if (campos.length < 11) {
            return null;
        }

        try {
            // Timestamp original do CSV → gravado como string
            String timestampStr = limparTimestamp(campos[2]);

            // CPU, RAM, DISCO → Double
            Double cpuPercent = parseDouble(campos[4]);
            Double ramPercent = parseDouble(campos[5]);
            Double discoPercent = parseDouble(campos[8]);

            return new ComponenteModel(
                    cpuPercent,
                    ramPercent,
                    discoPercent,
                    empresa,
                    servidor,
                    timestampStr
            );

        } catch (Exception e) {
            return null;
        }
    }

    private static Double parseDouble(String valor) {
        try {
            return Double.parseDouble(valor);
        } catch (Exception e) {
            return null;
        }
    }

    private static String limparTimestamp(String ts) {
        // Remove fração de segundos se existir
        if (ts.contains(".")) {
            ts = ts.substring(0, ts.indexOf('.'));
        }

        try {
            LocalDateTime.parse(ts, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            return ts;
        } catch (Exception e) {
            // Se der erro, devolve timestamp atual
            return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        }
    }
}
