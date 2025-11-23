package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.core.sync.RequestBody;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
        String bucketTrusted = "bucket-trusted-teste-tratamento";

        List<String> servidoresProcessados = new ArrayList<>();
        S3Manager s3Manager = new S3Manager(s3, bucketTrusted);

        String dataAtual = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        System.out.println("Processamento do dia: " + dataAtual);

        S3Object arquivoMachineData = s3Manager.encontrarArquivoDoDiaAtual(bucketRaw, "machine_data_", dataAtual);
        S3Object arquivoProcessos = s3Manager.encontrarArquivoDoDiaAtual(bucketRaw, "ProcessosUso_", dataAtual);

        if (arquivoMachineData == null && arquivoProcessos == null) {
            System.out.println("Nenhum arquivo do dia " + dataAtual + " encontrado no bucket raw. Encerrando processamento.");
            return;
        }

        if (arquivoMachineData != null) {
            System.out.println("Processando arquivo machine_data: " + arquivoMachineData.key());
            processarMachineData(s3, bucketRaw, arquivoMachineData.key(), con, alertaInsert,
                    servidoresProcessados, dataAtual, s3Manager);
        } else {
            System.out.println("Arquivo machine_data não encontrado para o dia " + dataAtual);
        }

        if (arquivoProcessos != null) {
            System.out.println("Processando arquivo de processos: " + arquivoProcessos.key());
            processarArquivoProcessos(s3, bucketRaw, arquivoProcessos.key(), dataAtual, s3Manager);
        } else {
            System.out.println("Arquivo de processos não encontrado para o dia " + dataAtual);
        }

        salvarArquivosTrusted(s3, bucketTrusted, arquivosPorServidor, dataAtual);

        System.out.println("Processamento concluído!");
        System.out.println("Servidores processados: " + servidoresProcessados);

        System.out.println("RELATORIO APOS PROCESSAMENTO...");
        RelatorioAlertas relatorio = new RelatorioAlertas(con, notificador);
        relatorio.gerarRelatorioAposProcessamento(servidoresProcessados);

        s3.close();
    }

    private static void processarMachineData(S3Client s3, String bucketRaw, String keyRaw,
                                             JdbcTemplate con, AlertaInsert alertaInsert,
                                             List<String> servidoresProcessados, String dataAtual,
                                             S3Manager s3Manager) {
        int linhasTotais = 0;
        int linhasProcessadas = 0;
        int alertasGerados = 0;

        try (InputStream rawStream = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(bucketRaw)
                        .key(keyRaw)
                        .build());
             BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                System.out.println("Arquivo machine_data vazio!");
                return;
            }

            System.out.println("Cabecalho machine_data: " + cabecalho);

            List<String[]> linhasParaProcessar = new ArrayList<>();
            String linha;
            while ((linha = leitor.readLine()) != null) {
                linhasTotais++;
                linha = limparLinha(linha);

                if (linha.trim().isEmpty()) continue;

                String[] campos = linha.split(";");
                if (campos.length >= 11) {
                    linhasParaProcessar.add(campos);
                    String servidorNomeCSV = campos[1].trim();
                    ServidorConfig servidor = buscarOuCriarServidor(con, servidorNomeCSV);
                    if (servidor != null && !servidoresProcessados.contains(servidor.getNome())) {
                        servidoresProcessados.add(servidor.getNome());
                    }
                }
            }

            carregarArquivosExistentes(s3Manager, dataAtual, cabecalho);

            for (String[] campos : linhasParaProcessar) {
                String servidorNomeCSV = campos[1].trim();
                ServidorConfig servidor = buscarOuCriarServidor(con, servidorNomeCSV);

                if (servidor != null) {
                    ComponenteLimites limites = buscarLimitesServidor(con, servidor.getId());
                    ServidorArquivo arquivoAtual = processarLinhaMachineData(campos, servidor, limites, alertaInsert, cabecalho);

                    if (arquivoAtual != null) {
                        boolean alertaGerado = verificarAlertasGerados(campos, servidor, limites, alertaInsert);
                        if (alertaGerado) alertasGerados++;
                    }
                    linhasProcessadas++;
                }
            }

            System.out.printf("Machine_data processado! Linhas lidas: %d Linhas válidas: %d Alertas gerados: %d%n",
                    linhasTotais, linhasProcessadas, alertasGerados);

        } catch (Exception e) {
            System.out.println("Erro ao processar machine_data: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processarArquivoProcessos(S3Client s3, String bucketRaw, String keyRaw,
                                                  String dataAtual, S3Manager s3Manager) {
        int linhasProcessos = 0;

        try (InputStream rawStream = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(bucketRaw)
                        .key(keyRaw)
                        .build());
             BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                System.out.println("Arquivo de processos vazio!");
                return;
            }

            System.out.println("Cabecalho processos: " + cabecalho);

            String linha;
            while ((linha = leitor.readLine()) != null) {
                linhasProcessos++;
                linha = limparLinha(linha);

                if (linha.trim().isEmpty()) continue;

                String[] campos = linha.split(";");
                if (campos.length >= 5) {
                    processarLinhaProcessos(campos);
                }
            }

            System.out.println("Arquivo de processos processado! Linhas lidas: " + linhasProcessos);

        } catch (Exception e) {
            System.out.println("Erro ao processar arquivo de processos: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void processarLinhaProcessos(String[] campos) {
        try {
            int id = Integer.parseInt(campos[0].trim());
            String servidorNome = campos[1].trim();
            String timestamp = formatarData(campos[2].trim());
            String nomeProcesso = campos[3].trim();
            double usoMemoria = Double.parseDouble(campos[4].trim());

            Processo processo = new Processo(id, servidorNome, timestamp, nomeProcesso, usoMemoria);

            ServidorArquivo arquivo = buscarArquivoPorServidor(servidorNome);
            if (arquivo != null) {
                arquivo.adicionarProcesso(processo);
            } else {
                System.out.println("Servidor não encontrado para processo: " + servidorNome);
            }

        } catch (NumberFormatException e) {
            System.out.println("Formato inválido na linha de processos: " + String.join(";", campos));
        }
    }

    private static ServidorArquivo buscarArquivoPorServidor(String servidorNome) {
        for (ServidorArquivo arquivo : arquivosPorServidor) {
            if (arquivo.getServidorNome().equals(servidorNome)) {
                return arquivo;
            }
        }
        return null;
    }

    private static void salvarArquivosTrusted(S3Client s3, String bucketTrusted,
                                              List<ServidorArquivo> arquivos, String data) {
        for (ServidorArquivo arquivo : arquivos) {
            salvarArquivoPrincipal(s3, bucketTrusted, arquivo, data);

            if (arquivo.temProcessos()) {
                salvarArquivoProcessos(s3, bucketTrusted, arquivo, data);
            }
        }
    }

    private static void salvarArquivoPrincipal(S3Client s3, String bucketTrusted, ServidorArquivo arquivo, String data) {
        try {
            String empresaFolder = formatarNome(arquivo.getEmpresaNome());
            String servidorFolder = formatarNome(arquivo.getServidorNome());

            String key = String.format("%s/%s/coleta_%s.csv",
                    empresaFolder, servidorFolder, data);

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketTrusted)
                            .key(key)
                            .contentType("text/csv")
                            .build(),
                    RequestBody.fromString(arquivo.getConteudoCSV())
            );

            System.out.println("Arquivo principal salvo: " + key);

        } catch (Exception e) {
            System.out.println("Erro ao salvar arquivo principal: " + e.getMessage());
        }
    }

    private static void salvarArquivoProcessos(S3Client s3, String bucketTrusted, ServidorArquivo arquivo, String data) {
        try {
            String empresaFolder = formatarNome(arquivo.getEmpresaNome());
            String servidorFolder = formatarNome(arquivo.getServidorNome());

            String key = String.format("%s/%s/ProcessosUso_%s.csv",
                    empresaFolder, servidorFolder, data);

            String csvProcessos = arquivo.gerarCSVProcessosOrdenado();

            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketTrusted)
                            .key(key)
                            .contentType("text/csv")
                            .build(),
                    RequestBody.fromString(csvProcessos)
            );

            System.out.println("Arquivo de processos salvo: " + key);

        } catch (Exception e) {
            System.out.println("Erro ao salvar arquivo de processos: " + e.getMessage());
        }
    }

    private static ServidorArquivo processarLinhaMachineData(String[] campos, ServidorConfig servidor,
                                                             ComponenteLimites limites, AlertaInsert alertaInsert,
                                                             String cabecalho) {

        String memoryAvailableGB = converterParaGB(campos[6].trim());
        String diskAvailableGB = converterParaGB(campos[9].trim());
        campos[6] = memoryAvailableGB;
        campos[9] = diskAvailableGB;

        String timestamp = formatarData(campos[2].trim());
        campos[2] = timestamp;

        String cpuStr = limparValor(campos[4].trim());
        String ramStr = limparValor(campos[5].trim());
        String discoStr = limparValor(campos[8].trim());

        String latenciaStr = formatarLatencia(campos[10].trim());
        campos[10] = latenciaStr;

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

        servidor.adicionarLeitura(gravidadeCpu, gravidadeRam, gravidadeDisco);

        ServidorArquivo arquivo = adicionarLinhaAoArquivoServidor(servidor, cabecalho, campos);
        return arquivo;
    }

    private static void carregarArquivosExistentes(S3Manager s3Manager, String data, String cabecalho) {
        System.out.println("Carregando arquivos existentes do bucket trusted para a data: " + data);

        if (servidoresConfig.isEmpty()) {
            System.out.println("Nenhum servidor identificado para carregar arquivos existentes.");
            return;
        }

        for (ServidorConfig servidor : servidoresConfig) {
            String empresaFolder = formatarNome(servidor.getEmpresaNome());
            String servidorFolder = formatarNome(servidor.getNome());

            String keyPrincipal = String.format("%s/%s/coleta_%s.csv",
                    empresaFolder, servidorFolder, data);

            String conteudoExistente = s3Manager.lerCSVExistente(keyPrincipal);

            ServidorArquivo arquivo = buscarOuCriarArquivoServidor(servidor);
            if (conteudoExistente != null) {
                arquivo.carregarConteudoExistente(conteudoExistente);
                System.out.println("Carregado arquivo existente: " + keyPrincipal);
            } else {
                System.out.println("Arquivo não encontrado (será criado): " + keyPrincipal);
                arquivo.adicionarLinha(cabecalho, "");
            }

            String keyProcessos = String.format("%s/%s/ProcessosUso_%s.csv",
                    empresaFolder, servidorFolder, data);

            String conteudoProcessosExistente = s3Manager.lerCSVExistente(keyProcessos);
            if (conteudoProcessosExistente != null) {
                arquivo.carregarProcessosExistente(conteudoProcessosExistente);
                System.out.println("Carregado arquivo de processos existente: " + keyProcessos);
            } else {
                System.out.println("Arquivo de processos não encontrado (será criado): " + keyProcessos);
                arquivo.adicionarLinhaProcessos("id;servidor;timestamp;NOME;USO_MEMORIA (MB)", "");
            }
        }
    }

    private static ServidorArquivo buscarOuCriarArquivoServidor(ServidorConfig servidor) {
        for (ServidorArquivo arquivo : arquivosPorServidor) {
            if (arquivo.getServidorNome().equals(servidor.getNome())) {
                return arquivo;
            }
        }
        ServidorArquivo novoArquivo = new ServidorArquivo(servidor.getEmpresaNome(), servidor.getNome());
        arquivosPorServidor.add(novoArquivo);
        return novoArquivo;
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
            SELECT tc.nome_tipo_componente as nome_tipo_componente, 
                   g.nome as gravidade, 
                   m.valor as valor
            FROM metrica m
            JOIN gravidade g ON m.fk_gravidade = g.id
            JOIN tipo_componente tc ON m.fk_componenteServidor_tipoComponente = tc.id
            WHERE m.fk_componenteServidor_servidor = ?
            ORDER BY tc.nome_tipo_componente, g.id
            """;

            List<MetricaLimite> metricas = con.query(sql, (rs, rowNum) ->
                    new MetricaLimite(
                            rs.getString("nome_tipo_componente"),
                            rs.getString("gravidade"),
                            rs.getDouble("valor")
                    ), servidorId);

            for (MetricaLimite metrica : metricas) {
                switch (metrica.getComponente()) {
                    case "Cpu":
                        if ("Baixo".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteBaixo(metrica.getValor());
                        } else if ("Médio".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteMedio(metrica.getValor());
                        } else if ("Alto".equals(metrica.getGravidade())) {
                            limites.setCpuLimiteAlto(metrica.getValor());
                        }
                        break;
                    case "Ram":
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
            Integer tipoComponenteId = alertaInsert.buscarIdTipoComponente("CPU");
            if (tipoComponenteId != null) {
                alertaInsert.inserirAlerta(servidor.getId(), tipoComponenteId, gravidadeCpu, timestamp);
                alertaGerado = true;
                servidor.resetarContadoresCpu();
                System.out.println("ALERTA CPU - Servidor: " + servidor.getNome() +
                        " | Gravidade: " + gravidadeCpu +
                        " | Leituras consecutivas: " + servidor.getLeiturasParaAlerta());
            }
        }

        if (servidor.deveAlertarRam(gravidadeRam)) {
            Integer tipoComponenteId = alertaInsert.buscarIdTipoComponente("RAM");
            if (tipoComponenteId != null) {
                alertaInsert.inserirAlerta(servidor.getId(), tipoComponenteId, gravidadeRam, timestamp);
                alertaGerado = true;
                servidor.resetarContadoresRam();
                System.out.println("ALERTA RAM - Servidor: " + servidor.getNome() +
                        " | Gravidade: " + gravidadeRam +
                        " | Leituras consecutivas: " + servidor.getLeiturasParaAlerta());
            }
        }

        if (servidor.deveAlertarDisco(gravidadeDisco)) {
            Integer tipoComponenteId = alertaInsert.buscarIdTipoComponente("Disco");
            if (tipoComponenteId != null) {
                alertaInsert.inserirAlerta(servidor.getId(), tipoComponenteId, gravidadeDisco, timestamp);
                alertaGerado = true;
                servidor.resetarContadoresDisco();
                System.out.println("ALERTA DISCO - Servidor: " + servidor.getNome() +
                        " | Gravidade: " + gravidadeDisco +
                        " | Leituras consecutivas: " + servidor.getLeiturasParaAlerta());
            }
        }

        return alertaGerado;
    }

    private static ServidorArquivo adicionarLinhaAoArquivoServidor(ServidorConfig servidor, String cabecalho, String[] campos) {
        ServidorArquivo arquivo = buscarArquivoServidor(servidor);

        if (arquivo == null) {
            arquivo = new ServidorArquivo(servidor.getEmpresaNome(), servidor.getNome());
            arquivosPorServidor.add(arquivo);
        }

        String linhaTratada = String.join(";", campos);
        arquivo.adicionarLinha(cabecalho, linhaTratada);

        return arquivo;
    }

    private static ServidorArquivo buscarArquivoServidor(ServidorConfig servidor) {
        for (ServidorArquivo arquivo : arquivosPorServidor) {
            if (arquivo.getServidorNome().equals(servidor.getNome())) {
                return arquivo;
            }
        }
        return null;
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

    private static String formatarLatencia(String latencia) {
        if (latencia == null || latencia.isEmpty() || latencia.equals("0.0") || latencia.equals("0")) {
            return "";
        }

        try {
            double latenciaValor = Double.parseDouble(latencia);
            return String.format("%.1f", latenciaValor);
        } catch (NumberFormatException e) {
            System.out.println("Valor inválido para latência: " + latencia);
            return "";
        }
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

    private static String formatarNome(String nome) {
        return nome.replaceAll("[^a-zA-Z0-9\\-_]", "_")
                .replaceAll("_{2,}", "_")
                .trim();
    }

    private static class MetricaLimite {
        private String componente;
        private String gravidade;
        private Double valor;

        public MetricaLimite(String componente, String gravidade, Double valor) {
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