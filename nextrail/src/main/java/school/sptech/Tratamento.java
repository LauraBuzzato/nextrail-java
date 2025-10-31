package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class Tratamento {

    private static final Map<Integer, List<Integer>> historicoGravidade = new HashMap<>();

    public static void main(String[] args) {

        BancoData banco = new BancoData();
        JdbcTemplate con = banco.con;

        MetricaData metricaData = new MetricaData(con);
        AlertaInsert alertaInsert = new AlertaInsert(con);
        Notificador notificador = new Notificador();

        String bucketRaw = "bucket-nr-prod-raw";           // Bucket de origem
        String keyRaw = "csv_grupo03_raw.csv";

        String bucketTrusted = "bucket-nr-prod-trusted";   // Bucket de destino
        String keyTrusted = "csv_grupo03_trusted.csv";

        int linhasTotais = 0;
        int linhasProcessadas = 0;
        int alertasGerados = 0;


        Integer servidorId = 1; // Servidor01
        Map<String, Integer> tipoComponenteMap = new HashMap<>();
        tipoComponenteMap.put("CPU", 1);
        tipoComponenteMap.put("Memória RAM", 2);
        tipoComponenteMap.put("Disco Rígido", 3);

        Map<Integer, Map<String, Map<String, Double>>> todosLimites =
                metricaData.buscarTodosLimitesServidor(servidorId);

        S3Client s3 = S3Client.create();

        try (
                InputStream rawStream = s3.getObject(
                        GetObjectRequest.builder()
                                .bucket(bucketRaw)
                                .key(keyRaw)
                                .build()
                );
                BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8));

                // Escrita do CSV tratado em memória
                StringWriter csvTratado = new StringWriter();
                BufferedWriter escritor = new BufferedWriter(csvTratado)
        ) {
            String linha = leitor.readLine();
            if (linha != null) escritor.write(linha + "\n"); // Cabeçalho

            while ((linha = leitor.readLine()) != null) {
                linhasTotais++;

                linha = limparLinha(linha);
                String[] campos = linha.split(",");

                if (campos.length >= 9) {
                    String id = campos[0].trim().toLowerCase();
                    String timestamp = formatarData(campos[1].trim());
                    String horarioDePico = campos[2].trim().equalsIgnoreCase("true") ? "1" : "0";
                    String cpu = limparValor(campos[3].trim());
                    String mem = limparValor(campos[4].trim());
                    String memAvail = limparGB(campos[5].trim());
                    String processos = limparProcessos(campos[6].trim());
                    String disk = limparValor(campos[7].trim());
                    String diskAvl = limparGB(campos[8].trim());

                    String[] linhaTratada = {id, timestamp, horarioDePico, cpu, mem, memAvail, processos, disk, diskAvl};
                    String linhaLimpa = String.join(",", linhaTratada);

                    if (linhaLimpa.isEmpty()) continue;

                    escritor.write(linhaLimpa + "\n");

                    // === PROCESSAMENTO DE ALERTAS ===
                    boolean gerouAlerta = false;

                    // Processar CPU
                    if (!cpu.isEmpty()) {
                        Integer tipoComponenteId = tipoComponenteMap.get("CPU");
                        if (tipoComponenteId != null) {
                            boolean alerta = verificarComponente(servidorId, tipoComponenteId, "CPU", cpu,
                                    todosLimites, alertaInsert, timestamp);
                            gerouAlerta = alerta || gerouAlerta;
                        }
                    }

                    // Processar Memória RAM
                    if (!mem.isEmpty()) {
                        Integer tipoComponenteId = tipoComponenteMap.get("Memória RAM");
                        if (tipoComponenteId != null) {
                            boolean alerta = verificarComponente(servidorId, tipoComponenteId, "Memória RAM", mem,
                                    todosLimites, alertaInsert, timestamp);
                            gerouAlerta = alerta || gerouAlerta;
                        }
                    }

                    // Processar Disco Rígido
                    if (!disk.isEmpty()) {
                        Integer tipoComponenteId = tipoComponenteMap.get("Disco Rígido");
                        if (tipoComponenteId != null) {
                            boolean alerta = verificarComponente(servidorId, tipoComponenteId, "Disco Rígido", disk,
                                    todosLimites, alertaInsert, timestamp);
                            gerouAlerta = alerta || gerouAlerta;
                        }
                    }

                    if (gerouAlerta) alertasGerados++;
                    linhasProcessadas++;
                }
            }

            escritor.flush();

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketTrusted)
                            .key(keyTrusted)
                            .build(),
                    RequestBody.fromString(csvTratado.toString())
            );

            System.out.printf("""
                    Processamento concluído!
                    Linhas lidas: %d
                    Linhas válidas: %d
                    Alertas gerados: %d
                    Arquivo tratado salvo em s3://%s/%s
                    """, linhasTotais, linhasProcessadas, alertasGerados, bucketTrusted, keyTrusted);

            System.out.println("\nEXECUTANDO TESTE DO RELATÓRIO...");
            executarTesteRelatorio(con, notificador);

        } catch (IOException e) {
            System.out.println("Erro ao processar CSV: " + e.getMessage());
        } finally {
            s3.close();
        }
    }


    private static void executarTesteRelatorio(JdbcTemplate con, Notificador notificador) {
        try {
            System.out.println("Gerando relatório de teste...");

            RelatorioAlertas relatorio = new RelatorioAlertas(con, notificador);
            relatorio.gerarRelatorioTeste();

            System.out.println("Teste do relatório concluído! Verifique Slack e Jira.");

        } catch (Exception e) {
            System.out.println("Erro ao executar teste do relatório: " + e.getMessage());
        }
    }

    private static String limparLinha(String linha) {
        if (linha == null) return "";
        linha = linha.replaceAll("[^\\x20-\\x7E]", "");
        linha = linha.replace(";", ",");
        return linha.trim();
    }

    private static String formatarData(String data) {
        try {
            SimpleDateFormat formatoEntrada = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS");
            SimpleDateFormat formatoSaida = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            data = data.replace("\"", "");
            return formatoSaida.format(formatoEntrada.parse(data));
        } catch (ParseException e) {
            return data;
        }
    }

    private static String limparValor(String valor) {
        if (valor.equals("0.0") || valor.equals("0") || valor.isEmpty()) return "";
        return valor;
    }

    private static String limparGB(String valor) {
        valor = valor.replace("G", "").replace("M", "").trim();
        try {
            Double.parseDouble(valor);
            return valor;
        } catch (NumberFormatException e) {
            return "";
        }
    }

    private static String limparProcessos(String valor) {
        valor = valor.replaceFirst("^0+(?!$)", "");
        try {
            Integer.parseInt(valor);
            return valor;
        } catch (NumberFormatException e) {
            return "";
        }
    }

    private static boolean verificarComponente(Integer servidorId, Integer tipoComponenteId,
                                               String nomeComponente, String valorStr,
                                               Map<Integer, Map<String, Map<String, Double>>> todosLimites,
                                               AlertaInsert alertaInsert, String timestamp) {
        if (valorStr == null || valorStr.isEmpty()) return false;

        try {
            double valorLido = Double.parseDouble(valorStr);

            Map<String, Map<String, Double>> limitesComponente = todosLimites.get(tipoComponenteId);
            if (limitesComponente == null) {
                System.out.printf("Nenhum limite encontrado para tipo componente ID: %d%n", tipoComponenteId);
                return false;
            }

            String nomeMetrica;
            switch (nomeComponente) {
                case "CPU" -> nomeMetrica = "Uso de CPU";
                case "Memória RAM" -> nomeMetrica = "Uso de RAM";
                case "Disco Rígido" -> nomeMetrica = "Uso de Disco";
                default -> nomeMetrica = nomeComponente;
            }

            Map<String, Double> limitesMetrica = limitesComponente.get(nomeMetrica);
            if (limitesMetrica == null) {
                System.out.printf("Métrica %s não encontrada para componente %s%n", nomeMetrica, nomeComponente);
                return false;
            }

            Integer gravidadeId = 0;
            String gravidadeEncontrada = "Normal";
            Double limiteAtingido = null;

            if (limitesMetrica.containsKey("Alto") && valorLido >= limitesMetrica.get("Alto")) {
                gravidadeId = 3;
                gravidadeEncontrada = "Alto";
                limiteAtingido = limitesMetrica.get("Alto");
            } else if (limitesMetrica.containsKey("Médio") && valorLido >= limitesMetrica.get("Médio")) {
                gravidadeId = 2;
                gravidadeEncontrada = "Médio";
                limiteAtingido = limitesMetrica.get("Médio");
            } else if (limitesMetrica.containsKey("Baixo") && valorLido >= limitesMetrica.get("Baixo")) {
                gravidadeId = 1;
                gravidadeEncontrada = "Baixo";
                limiteAtingido = limitesMetrica.get("Baixo");
            }

            System.out.printf("Servidor: %d | Componente: %s | Valor: %.2f | Gravidade: %s | Limite: %s | horário: %s%n",
                    servidorId, nomeComponente, valorLido, gravidadeEncontrada, limiteAtingido, timestamp);

            historicoGravidade.putIfAbsent(tipoComponenteId, new LinkedList<>());
            List<Integer> historico = historicoGravidade.get(tipoComponenteId);

            historico.add(gravidadeId);
            if (historico.size() > 3) ((LinkedList<Integer>) historico).removeFirst();

            if (historico.size() == 3 && gravidadeId > 0) {
                Integer g1 = historico.get(0);
                Integer g2 = historico.get(1);
                Integer g3 = historico.get(2);

                if (g1.equals(g2) && g2.equals(g3)) {
                    alertaInsert.inserirAlerta(servidorId, tipoComponenteId, gravidadeId, timestamp);
                    System.out.printf("ALERTA INSERIDO NO BANCO (3 CONSECUTIVOS): Servidor %d - %s - %s (%.2f >= %.2f)%n",
                            servidorId, nomeComponente, gravidadeEncontrada, valorLido, limiteAtingido);
                    return true;
                }
            }

            if (gravidadeId == 0) {
                System.out.printf("%s dentro do limite normal (%.2f)%n", nomeComponente, valorLido);
            }

        } catch (NumberFormatException e) {
            System.out.println("Valor inválido para " + nomeComponente + ": " + valorStr);
        }

        return false;
    }
}
