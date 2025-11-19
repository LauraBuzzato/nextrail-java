package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class Tratamento {

    private static List<ServidorConfig> servidoresConfig = new ArrayList<>();
    private static List<ServidorArquivo> arquivosPorServidor = new ArrayList<>();

    public static void main(String[] args) {
        BancoData banco = new BancoData();
        JdbcTemplate con = banco.con;
        AlertaInsert alertaInsert = new AlertaInsert(con);
        Notificador notificador = new Notificador();
        S3Client s3 = S3Client.create();

        String bucketRaw = "bucket-teste-python";
        String keyRaw = "machine_data_2025-11-19.csv";
        String bucketTrusted = "bucket-trusted-teste-tratamento";

        int linhasTotais = 0;
        int linhasProcessadas = 0;
        int alertasGerados = 0;

        ServidorArquivo arquivoAtual = null;

        try (
                InputStream rawStream = s3.getObject(
                        GetObjectRequest.builder()
                                .bucket(bucketRaw)
                                .key(keyRaw)
                                .build()
                );
                BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8));
        ) {
            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                System.out.println("Arquivo vazio!");
                return;
            }

            String linha;
            while ((linha = leitor.readLine()) != null) {
                linhasTotais++;
                linha = limparLinha(linha);
                String[] campos = linha.split(";");

                if (campos.length >= 11) {
                    String servidorNomeCSV = campos[1].trim();
                    ServidorConfig servidor = buscarOuCriarServidor(con, servidorNomeCSV);

                    if (servidor != null) {
                        ComponenteLimites limites = buscarLimitesServidor(con, servidor.getId());
                        arquivoAtual = processarLinha(campos, servidor, limites, alertaInsert, cabecalho);

                        if (arquivoAtual != null) {
                            boolean alertaGerado = verificarAlertasGerados(campos, servidor, limites, alertaInsert);
                            if (alertaGerado) alertasGerados++;
                        }
                        linhasProcessadas++;
                    }
                }
            }

            if (arquivoAtual != null) {
                S3Manager s3Manager = new S3Manager(s3, bucketTrusted);
                s3Manager.salvarCSVTrusted(
                        arquivoAtual.getEmpresaNome(),
                        arquivoAtual.getServidorNome(),
                        arquivoAtual.getConteudoCSV()
                );
                System.out.println("Arquivo salvo: " + arquivoAtual.getEmpresaNome() + "/" + arquivoAtual.getServidorNome());
            }

            System.out.printf("Processamento concluído! Linhas lidas: %d Linhas válidas: %d Alertas gerados: %d%n",
                    linhasTotais, linhasProcessadas, alertasGerados);

            System.out.println("EXECUTANDO TESTE DO RELATÓRIO...");
            executarTesteRelatorio(con, notificador);

        } catch (Exception e) {
            System.out.println("Erro ao processar CSV: " + e.getMessage());
            e.printStackTrace();
        } finally {
            s3.close();
        }
    }

    private static ServidorConfig buscarOuCriarServidor(JdbcTemplate con, String servidorNome) {
        for (ServidorConfig servidor : servidoresConfig) {
            if (servidor.getNome().equals(servidorNome)) {
                return servidor;
            }
        }
        ServidorConfig novoServidor = buscarServidorNoBanco(con, servidorNome);
        if (novoServidor != null) {
            servidoresConfig.add(novoServidor);
        }
        return novoServidor;
    }

    private static ServidorConfig buscarServidorNoBanco(JdbcTemplate con, String servidorNome) {
        try {
            String sql = """
                SELECT DISTINCT s.id as id, s.nome as nome, e.razao_social as empresa_nome, 
                       e.id as empresa_id, ls.leituras_consecutivas_para_alerta as leituras_consecutivas_para_alerta
                FROM servidor s
                JOIN empresa e ON s.fk_empresa = e.id
                JOIN leitura_script ls ON ls.fk_servidor = s.id
                WHERE s.nome = ?
                LIMIT 1
                """;

            List<ServidorConfig> resultados = con.query(sql, (rs, rowNum) ->
                    new ServidorConfig(
                            rs.getInt("id"),
                            rs.getString("nome"),
                            rs.getString("empresa_nome"),
                            rs.getInt("empresa_id"),
                            rs.getInt("leituras_consecutivas_para_alerta")
                    ), servidorNome);

            return resultados.isEmpty() ? null : resultados.get(0);

        } catch (Exception e) {
            System.out.println("Erro ao buscar servidor '" + servidorNome + "': " + e.getMessage());
            return null;
        }
    }

    private static ComponenteLimites buscarLimitesServidor(JdbcTemplate con, int servidorId) {
        ComponenteLimites limites = new ComponenteLimites();
        try {
            String sql = """
                SELECT tc.nome_tipo_componente as nome_tipo_componente, g.nome as gravidade, m.valor as valor
                FROM metrica m
                JOIN gravidade g ON m.fk_gravidade = g.id
                JOIN tipo_componente tc ON m.fk_componenteServidor_tipoComponente = tc.id
                WHERE m.fk_componenteServidor_servidor = ?
                ORDER BY tc.nome_tipo_componente, g.nome
                """;

            List<MetricaLimite> metricas = con.query(sql, (rs, rowNum) ->
                    new MetricaLimite(
                            rs.getString("nome_tipo_componente"),
                            rs.getString("gravidade"),
                            rs.getDouble("valor")
                    ), servidorId);

            for (MetricaLimite metrica : metricas) {
                switch (metrica.getComponente()) {
                    case "CPU":
                        if ("Baixo".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteBaixo(metrica.getValor());
                        } else if ("Médio".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteMedio(metrica.getValor());
                        } else if ("Alto".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteAlto(metrica.getValor());
                        }
                        break;
                    case "RAM":
                        if ("Baixo".equals(metrica.getGravidade())) {
                            limites.setRamLimiteBaixo(metrica.getValor());
                        } else if ("Médio".equals(metrica.getGravidade())) {
                            limites.setRamLimiteMedio(metrica.getValor());
                        } else if ("Alto".equals(metrica.getGravidade())) {
                            limites.setRamLimiteAlto(metrica.getValor());
                        }
                        break;
                    case "Disco":
                        if ("Baixo".equals(metrica.getGravidade())) {
                            limites.setDiscoLimiteBaixo(metrica.getValor());
                        } else if ("Médio".equals(metrica.getGravidade())) {
                            limites.setDiscoLimiteMedio(metrica.getValor());
                        } else if ("Alto".equals(metrica.getGravidade())) {
                            limites.setDiscoLimiteAlto(metrica.getValor());
                        }
                        break;
                }
            }

            System.out.printf("Limites carregados - CPU: %.1f/%.1f/%.1f, RAM: %.1f/%.1f/%.1f, Disco: %.1f/%.1f/%.1f%n",
                    limites.getCpuLimiteBaixo(), limites.getCpuLimiteMedio(), limites.getCpuLimiteAlto(),
                    limites.getRamLimiteBaixo(), limites.getRamLimiteMedio(), limites.getRamLimiteAlto(),
                    limites.getDiscoLimiteBaixo(), limites.getDiscoLimiteMedio(), limites.getDiscoLimiteAlto());

        } catch (Exception e) {
            System.out.println("Erro ao buscar limites: " + e.getMessage());
            e.printStackTrace();
        }
        return limites;
    }

    private static ServidorArquivo processarLinha(String[] campos, ServidorConfig servidor,
                                                  ComponenteLimites limites, AlertaInsert alertaInsert,
                                                  String cabecalho) {

        String memoryAvailableGB = converterParaGB(campos[6].trim());
        String diskAvailableGB = converterParaGB(campos[9].trim());
        campos[6] = memoryAvailableGB;
        campos[9] = diskAvailableGB;

        String timestamp = formatarData(campos[2].trim());
        String cpuStr = limparValor(campos[4].trim());
        String ramStr = limparValor(campos[5].trim());
        String discoStr = limparValor(campos[8].trim());

        int gravidadeCpu = 0;
        int gravidadeRam = 0;
        int gravidadeDisco = 0;

        if (!cpuStr.isEmpty()) {
            try {
                double cpuValor = Double.parseDouble(cpuStr);
                gravidadeCpu = limites.verificarGravidadeCpu(cpuValor);
                System.out.printf("CPU: %.1f%% → Gravidade: %d%n", cpuValor, gravidadeCpu);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para CPU: " + cpuStr);
            }
        }

        if (!ramStr.isEmpty()) {
            try {
                double ramValor = Double.parseDouble(ramStr);
                gravidadeRam = limites.verificarGravidadeRam(ramValor);
                System.out.printf("RAM: %.1f%% → Gravidade: %d%n", ramValor, gravidadeRam);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para RAM: " + ramStr);
            }
        }

        if (!discoStr.isEmpty()) {
            try {
                double discoValor = Double.parseDouble(discoStr);
                gravidadeDisco = limites.verificarGravidadeDisco(discoValor);
                System.out.printf("Disco: %.1f%% → Gravidade: %d%n", discoValor, gravidadeDisco);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para Disco: " + discoStr);
            }
        }

        servidor.adicionarLeitura(gravidadeCpu, gravidadeRam, gravidadeDisco);

        ServidorArquivo arquivo = adicionarLinhaAoArquivoServidor(servidor, cabecalho, campos);
        return arquivo;
    }

    private static boolean verificarAlertasGerados(String[] campos, ServidorConfig servidor,
                                                   ComponenteLimites limites, AlertaInsert alertaInsert) {
        String timestamp = formatarData(campos[2].trim());
        String cpuStr = limparValor(campos[4].trim());
        String ramStr = limparValor(campos[5].trim());
        String discoStr = limparValor(campos[8].trim());

        boolean alertaGerado = false;
        int gravidadeCpu = 0;
        int gravidadeRam = 0;
        int gravidadeDisco = 0;

        if (!cpuStr.isEmpty()) {
            try {
                double cpuValor = Double.parseDouble(cpuStr);
                gravidadeCpu = limites.verificarGravidadeCpu(cpuValor);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para CPU: " + cpuStr);
            }
        }

        if (!ramStr.isEmpty()) {
            try {
                double ramValor = Double.parseDouble(ramStr);
                gravidadeRam = limites.verificarGravidadeRam(ramValor);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para RAM: " + ramStr);
            }
        }

        if (!discoStr.isEmpty()) {
            try {
                double discoValor = Double.parseDouble(discoStr);
                gravidadeDisco = limites.verificarGravidadeDisco(discoValor);
            } catch (NumberFormatException e) {
                System.out.println("Valor inválido para Disco: " + discoStr);
            }
        }

        if (servidor.deveAlertarCpu(gravidadeCpu)) {
            alertaInsert.inserirAlerta(servidor.getId(), 1, gravidadeCpu, timestamp);
            alertaGerado = true;
            System.out.println("ALERTA CPU - Servidor: " + servidor.getNome() + " | Gravidade: " + gravidadeCpu);
        }

        if (servidor.deveAlertarRam(gravidadeRam)) {
            alertaInsert.inserirAlerta(servidor.getId(), 2, gravidadeRam, timestamp);
            alertaGerado = true;
            System.out.println("ALERTA RAM - Servidor: " + servidor.getNome() + " | Gravidade: " + gravidadeRam);
        }

        if (servidor.deveAlertarDisco(gravidadeDisco)) {
            alertaInsert.inserirAlerta(servidor.getId(), 3, gravidadeDisco, timestamp);
            alertaGerado = true;
            System.out.println("ALERTA DISCO - Servidor: " + servidor.getNome() + " | Gravidade: " + gravidadeDisco);
        }

        return alertaGerado;
    }

    private static ServidorArquivo adicionarLinhaAoArquivoServidor(ServidorConfig servidor, String cabecalho, String[] campos) {
        ServidorArquivo arquivo = null;
        for (ServidorArquivo arq : arquivosPorServidor) {
            if (arq.getServidorNome().equals(servidor.getNome())) {
                arquivo = arq;
                break;
            }
        }

        if (arquivo == null) {
            arquivo = new ServidorArquivo(servidor.getEmpresaNome(), servidor.getNome());
            arquivosPorServidor.add(arquivo);
        }

        String linhaTratada = String.join(";", campos);
        arquivo.adicionarLinha(cabecalho, linhaTratada);

        return arquivo;
    }

    private static void executarTesteRelatorio(JdbcTemplate con, Notificador notificador) {
        try {
            System.out.println("Gerando relatório de teste...");
            RelatorioAlertas relatorio = new RelatorioAlertas(con, notificador);
            relatorio.gerarRelatorioTeste();
            System.out.println("Teste do relatório concluído!");
        } catch (Exception e) {
            System.out.println("Erro ao executar teste do relatório: " + e.getMessage());
        }
    }

    private static String limparLinha(String linha) {
        if (linha == null) return "";
        linha = linha.replaceAll("[^\\x20-\\x7E]", "");
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

    private static String converterParaGB(String valorFormat) {
        if (valorFormat == null || valorFormat.isEmpty()) return "";

        try {
            valorFormat = valorFormat.toUpperCase().trim();
            double valor;

            if (valorFormat.endsWith("G")) {
                valor = Double.parseDouble(valorFormat.replace("G", "").trim());
            } else if (valorFormat.endsWith("M")) {
                valor = Double.parseDouble(valorFormat.replace("M", "").trim()) / 1024.0;
            } else if (valorFormat.endsWith("K")) {
                valor = Double.parseDouble(valorFormat.replace("K", "").trim()) / (1024.0 * 1024.0);
            } else {
                valor = Double.parseDouble(valorFormat) / (1024.0 * 1024.0 * 1024.0);
            }

            return String.format("%.2f", valor);
        } catch (NumberFormatException e) {
            System.out.println("Valor inválido para conversão GB: " + valorFormat);
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

    private static class MetricaLimite {
        private String componente;
        private String gravidade;
        private double valor;

        public MetricaLimite(String componente, String gravidade, double valor) {
            this.componente = componente;
            this.gravidade = gravidade;
            this.valor = valor;
        }

        public String getComponente() {
            return componente;
        }

        public String getGravidade() {
            return gravidade;
        }

        public double getValor() {
            return valor;
        }
    }
}