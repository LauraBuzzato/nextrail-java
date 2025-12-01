package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.springframework.jdbc.core.JdbcTemplate;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class Tratamento implements RequestHandler<Map<String, Object>, String> {

    private static final List<ServidorConfig> servidoresConfig = new ArrayList<>();
    private static final List<ServidorArquivo> arquivosPorServidor = new ArrayList<>();

    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        LambdaLogger logger = context.getLogger();
        logger.log("Iniciando processamento ETL via Lambda");

        try {
            String resultado = processarComIntervalo();
            logger.log("Processamento concluído");
            return resultado;
        } catch (Exception e) {
            logger.log("Erro fatal no processamento: " + e.getMessage());
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }

    public static String processarComIntervalo() {
        try {
            BancoData banco = new BancoData();
            JdbcTemplate con = banco.con;

            LocalDateTime ultimaExecucao = buscarUltimaExecucao(con);

            if (!deveExecutar(ultimaExecucao)) {
                long minutosPassados = java.time.temporal.ChronoUnit.MINUTES.between(ultimaExecucao, LocalDateTime.now());
                System.out.println("Ainda não passou 1 hora desde a última execução. Minutos desde última execução: " + minutosPassados);
                return "SKIPPED: Ainda não passou 1 hora desde a última execução. " + minutosPassados + " minutos desde última execução.";
            }

            System.out.println("Passou 1 hora desde a última execução. Iniciando processamento...");

            atualizarUltimaExecucao(con);

            return processar();

        } catch (Exception e) {
            System.err.println("Erro no controle de intervalo: " + e.getMessage());
            e.printStackTrace();
            return "ERROR_NO_INTERVALO: " + e.getMessage();
        }
    }

    private static boolean deveExecutar(LocalDateTime ultimaExecucao) {
        if (ultimaExecucao == null) {
            return true;
        }

        long minutosPassados = java.time.temporal.ChronoUnit.MINUTES.between(ultimaExecucao, LocalDateTime.now());
        return minutosPassados >= 60;
    }

    private static LocalDateTime buscarUltimaExecucao(JdbcTemplate con) {
        try {
            criarTabelaControleSeNecessario(con);

            String sql = "SELECT ultima_execucao FROM controle_execucao_etl WHERE id = 1";

            try {
                String ultimaExecucaoStr = con.queryForObject(sql, String.class);
                if (ultimaExecucaoStr != null) {
                    return LocalDateTime.parse(ultimaExecucaoStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                }
            } catch (Exception e) {
                System.out.println("Primeira execução ou tabela vazia");
            }

            return null;

        } catch (Exception e) {
            System.err.println("Erro ao buscar última execução: " + e.getMessage());
            return null;
        }
    }

    private static void atualizarUltimaExecucao(JdbcTemplate con) {
        try {
            String horarioAtual = LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);

            String sql = "INSERT INTO controle_execucao_etl (id, ultima_execucao, data_atualizacao) VALUES (1, ?, NOW()) ON DUPLICATE KEY UPDATE ultima_execucao = ?, data_atualizacao = NOW()";

            con.update(sql, horarioAtual, horarioAtual);
            System.out.println("Última execução atualizada para: " + horarioAtual);

        } catch (Exception e) {
            System.err.println("Erro ao atualizar última execução: " + e.getMessage());
        }
    }

    private static void criarTabelaControleSeNecessario(JdbcTemplate con) {
        try {
            String sql = "CREATE TABLE IF NOT EXISTS controle_execucao_etl (id INT PRIMARY KEY, ultima_execucao DATETIME NULL, data_atualizacao DATETIME NOT NULL)";

            con.execute(sql);
            System.out.println("Tabela de controle verificada/criada");

        } catch (Exception e) {
            System.err.println("Erro ao criar tabela de controle: " + e.getMessage());
        }
    }

    public static String processar() {
        return processarComData(LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd")));
    }

    public static String processarComData(String dataEspecifica) {
        StringBuilder resultado = new StringBuilder();

        servidoresConfig.clear();
        arquivosPorServidor.clear();

        S3Client s3 = null;
        try {
            System.out.println("INICIANDO PROCESSAMENTO ETL");
            resultado.append("INICIANDO PROCESSAMENTO ETL\n");

            BancoData banco = new BancoData();
            JdbcTemplate con = banco.con;
            AlertaInsert alertaInsert = new AlertaInsert(con);
            Notificador notificador = new Notificador();

            s3 = S3Client.builder()
                    .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                    .build();

            String bucketRaw = System.getenv("BUCKET_RAW");
            String bucketTrusted = System.getenv("BUCKET_TRUSTED");
            String bucketClient = System.getenv("BUCKET_CLIENT");

            if (bucketRaw == null) bucketRaw = "raw-nextrail-teste";
            if (bucketTrusted == null) bucketTrusted = "trusted-nextrail-teste";
            if (bucketClient == null) bucketClient = "client-nextrail-teste";

            System.out.println("Buckets configurados:");
            System.out.println("Raw: " + bucketRaw);
            System.out.println("Trusted: " + bucketTrusted);
            System.out.println("Client: " + bucketClient);

            resultado.append("Buckets configurados:\n")
                    .append("Raw: ").append(bucketRaw).append("\n")
                    .append("Trusted: ").append(bucketTrusted).append("\n")
                    .append("Client: ").append(bucketClient).append("\n");

            List<String> servidoresProcessados = new ArrayList<>();
            S3Manager s3Manager = new S3Manager(s3, bucketTrusted);

            String dataAtual = dataEspecifica != null ? dataEspecifica :
                    LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            System.out.println("Processamento do dia: " + dataAtual);
            resultado.append("Processamento do dia: ").append(dataAtual).append("\n");

            S3Object arquivoMachineData = s3Manager.encontrarArquivoDoDiaAtual(bucketRaw, "machine_data_", dataAtual);
            S3Object arquivoProcessos = s3Manager.encontrarArquivoDoDiaAtual(bucketRaw, "nomeProcessosUso_", dataAtual);

            if (arquivoMachineData == null && arquivoProcessos == null) {
                String mensagem = "Nenhum arquivo do dia " + dataAtual + " encontrado no bucket raw. Encerrando processamento.";
                System.out.println(mensagem);
                resultado.append(mensagem).append("\n");
                s3.close();
                return resultado.toString();
            }

            if (arquivoMachineData != null) {
                System.out.println("Processando arquivo machine_data: " + arquivoMachineData.key());
                resultado.append("Processando arquivo machine_data: ").append(arquivoMachineData.key()).append("\n");

                try {
                    String resultadoMachineData = processarMachineData(s3, bucketRaw, arquivoMachineData.key(), con, alertaInsert,
                            servidoresProcessados, dataAtual, s3Manager);
                    resultado.append(resultadoMachineData).append("\n");
                } catch (Exception e) {
                    String erro = "Erro ao processar machine_data: " + e.getMessage();
                    System.err.println(erro);
                    resultado.append(erro).append("\n");
                }
            } else {
                String mensagem = "Arquivo machine_data não encontrado para o dia " + dataAtual;
                System.out.println(mensagem);
                resultado.append(mensagem).append("\n");
            }

            if (arquivoProcessos != null) {
                System.out.println("Processando arquivo de processos: " + arquivoProcessos.key());
                resultado.append("Processando arquivo de processos: ").append(arquivoProcessos.key()).append("\n");

                try {
                    String resultadoProcessos = processarArquivoProcessos(s3, bucketRaw, arquivoProcessos.key(), dataAtual, s3Manager);
                    resultado.append(resultadoProcessos).append("\n");
                } catch (Exception e) {
                    String erro = "Erro ao processar arquivo de processos: " + e.getMessage();
                    System.err.println(erro);
                    resultado.append(erro).append("\n");
                }
            } else {
                String mensagem = "Arquivo de processos não encontrado para o dia " + dataAtual;
                System.out.println(mensagem);
                resultado.append(mensagem).append("\n");
            }

            if (!arquivosPorServidor.isEmpty()) {
                try {
                    salvarArquivosTrusted(s3, bucketTrusted, bucketClient, arquivosPorServidor, dataAtual);
                    resultado.append("Arquivos salvos no bucket trusted com sucesso\n");
                } catch (Exception e) {
                    String erro = "Erro ao salvar arquivos trusted: " + e.getMessage();
                    System.err.println(erro);
                    resultado.append(erro).append("\n");
                }
            } else {
                resultado.append("Nenhum arquivo para salvar no bucket trusted\n");
            }

            System.out.println("Processamento concluído!");
            resultado.append("Processamento concluído!\n");
            System.out.println("Servidores processados: " + servidoresProcessados);
            resultado.append("Servidores processados: ").append(servidoresProcessados.toString()).append("\n");

            if (!servidoresProcessados.isEmpty()) {
                System.out.println("RELATORIO APOS PROCESSAMENTO...");
                resultado.append("RELATORIO APOS PROCESSAMENTO...\n");

                try {
                    RelatorioAlertas relatorio = new RelatorioAlertas(con, notificador);
                    String resultadoRelatorio = relatorio.gerarRelatorioAposProcessamento(servidoresProcessados);
                    resultado.append(resultadoRelatorio).append("\n");
                } catch (Exception e) {
                    String erroRelatorio = "Erro ao gerar relatório: " + e.getMessage();
                    System.err.println(erroRelatorio);
                    resultado.append(erroRelatorio).append("\n");
                }
            } else {
                resultado.append("Nenhum servidor processado, relatório não gerado\n");
            }

            s3.close();
            resultado.append("CONCLUÍDO\n");

        } catch (Exception e) {
            String erro = "Erro durante o processamento ETL: " + e.getMessage();
            System.err.println(erro);
            e.printStackTrace();
            resultado.append("ERRO: ").append(erro).append("\n");
            resultado.append("Stack trace: ").append(Arrays.toString(e.getStackTrace())).append("\n");
        } finally {
            servidoresConfig.clear();
            arquivosPorServidor.clear();
        }

        return resultado.toString();
    }

    private static String processarMachineData(S3Client s3, String bucketRaw, String keyRaw,
                                               JdbcTemplate con, AlertaInsert alertaInsert,
                                               List<String> servidoresProcessados, String dataAtual,
                                               S3Manager s3Manager) {
        StringBuilder resultado = new StringBuilder();
        int linhasTotais = 0;
        int linhasProcessadas = 0;
        int linhasNovas = 0;
        int alertasGerados = 0;

        System.out.println("START ");
        try (InputStream rawStream = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(bucketRaw)
                        .key(keyRaw)
                        .build());
             BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                resultado.append("Arquivo machine_data vazio!\n");
                return resultado.toString();
            }

            System.out.println("Cabecalho machine_data: " + cabecalho);
            resultado.append("Cabecalho machine_data carregado\n");

            carregarUltimosTimestamps(s3Manager, dataAtual, cabecalho);

            List<String[]> linhasParaProcessar = new ArrayList<>();
            String linha;
            while ((linha = leitor.readLine()) != null) {
                linhasTotais++;
                if (linhasTotais % 100 == 0) {
                    System.out.println("Linhas lidas: " + linhasTotais);
                }

                linha = limparLinha(linha);

                if (linha.trim().isEmpty()) continue;

                String[] campos = linha.split(";");
                if (campos.length >= 11) {
                    String servidorNomeCSV = campos[1].trim();
                    String timestampRaw = campos[2].trim();

                    System.out.println("Processando servidor: " + servidorNomeCSV + " | Timestamp: " + timestampRaw);

                    if (ehLinhaNova(servidorNomeCSV, timestampRaw)) {
                        linhasParaProcessar.add(campos);
                        linhasNovas++;

                        ServidorConfig servidor = buscarOuCriarServidor(con, servidorNomeCSV);
                        if (servidor != null && !servidoresProcessados.contains(servidor.getNome())) {
                            servidoresProcessados.add(servidor.getNome());
                            System.out.println("Servidor adicionado para processamento: " + servidor.getNome());
                        } else if (servidor == null) {
                            System.out.println("AVISO: Servidor não encontrado no banco: " + servidorNomeCSV);
                        }
                    } else {
                        System.out.println("Linha duplicada ignorada - Servidor: " + servidorNomeCSV + " | Timestamp: " + timestampRaw);
                    }
                } else {
                    System.out.println("Linha ignorada - campos insuficientes: " + linha);
                }
            }

            System.out.println("Total de linhas para processar: " + linhasParaProcessar.size());

            for (int i = 0; i < linhasParaProcessar.size(); i++) {
                String[] campos = linhasParaProcessar.get(i);
                String servidorNomeCSV = campos[1].trim();
                ServidorConfig servidor = buscarOuCriarServidor(con, servidorNomeCSV);

                if (servidor != null) {
                    System.out.println("Processando linha " + (i + 1) + "/" + linhasParaProcessar.size() +
                            " - Servidor: " + servidor.getNome());

                    ComponenteLimites limites = buscarLimitesServidor(con, servidor.getId());
                    ServidorArquivo arquivoAtual = processarLinhaMachineData(campos, servidor, limites, alertaInsert, cabecalho);

                    if (arquivoAtual != null) {
                        boolean alertaGerado = verificarAlertasGerados(campos, servidor, limites, alertaInsert);
                        if (alertaGerado) alertasGerados++;
                    }
                    linhasProcessadas++;
                } else {
                    System.out.println("ERRO: Servidor não encontrado para linha - " + servidorNomeCSV);
                }
            }

            String resumo = String.format("Machine_data processado! Linhas lidas: %d | Linhas novas: %d | Linhas processadas: %d | Alertas gerados: %d | Servidores processados: %d",
                    linhasTotais, linhasNovas, linhasProcessadas, alertasGerados, servidoresProcessados.size());
            System.out.println(resumo);
            resultado.append(resumo).append("\n");

            if (!servidoresProcessados.isEmpty()) {
                resultado.append("Servidores processados: ").append(String.join(", ", servidoresProcessados)).append("\n");
            } else {
                resultado.append("NENHUM servidor foi processado - verifique os nomes no CSV vs Banco de Dados\n");
            }

        } catch (Exception e) {
            String erro = "Erro ao processar machine_data: " + e.getMessage();
            System.out.println(erro);
            e.printStackTrace();
            resultado.append(erro).append("\n");
            resultado.append("Stack trace: ").append(Arrays.toString(e.getStackTrace())).append("\n");
        }

        return resultado.toString();
    }

    private static void carregarUltimosTimestamps(S3Manager s3Manager, String data, String cabecalho) {
        System.out.println("Carregando últimos timestamps do bucket trusted para a data: " + data);

        if (servidoresConfig.isEmpty()) {
            System.out.println("Nenhum servidor identificado para carregar timestamps.");
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
                String ultimoTimestamp = extrairUltimoTimestamp(conteudoExistente, servidor.getNome());
                arquivo.setUltimoTimestamp(ultimoTimestamp);
                System.out.println("Último timestamp para " + servidor.getNome() + ": " + ultimoTimestamp);
            } else {
                System.out.println("Arquivo não encontrado (será criado): " + keyPrincipal);
                arquivo.adicionarLinha(cabecalho, "");
                arquivo.setUltimoTimestamp(null);
            }
        }
    }

    private static String extrairUltimoTimestamp(String conteudoCSV, String servidorNome) {
        if (conteudoCSV == null || conteudoCSV.isEmpty()) {
            return null;
        }

        String[] linhas = conteudoCSV.split("\n");
        String ultimoTimestamp = null;

        for (int i = linhas.length - 1; i >= 0; i--) {
            String linha = linhas[i].trim();
            if (!linha.isEmpty() && !linha.startsWith("id;")) {
                String[] campos = linha.split(";");
                if (campos.length >= 3 && campos[1].equals(servidorNome)) {
                    ultimoTimestamp = campos[2];
                    break;
                }
            }
        }
        return ultimoTimestamp;
    }

    private static boolean ehLinhaNova(String servidorNome, String timestampRaw) {
        ServidorArquivo arquivo = buscarArquivoPorServidor(servidorNome);
        if (arquivo == null) {
            return true;
        }

        String ultimoTimestamp = arquivo.getUltimoTimestamp();
        if (ultimoTimestamp == null) {
            return true;
        }

        try {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date dataRaw = format.parse(formatarData(timestampRaw));
            Date dataUltima = format.parse(ultimoTimestamp);

            return dataRaw.after(dataUltima);
        } catch (ParseException e) {
            System.out.println("Erro ao comparar timestamps: " + e.getMessage());
            return true;
        }
    }

    private static String processarArquivoProcessos(S3Client s3, String bucketRaw, String keyRaw,
                                                    String dataAtual, S3Manager s3Manager) {
        StringBuilder resultado = new StringBuilder();
        int linhasProcessos = 0;

        try (InputStream rawStream = s3.getObject(
                GetObjectRequest.builder()
                        .bucket(bucketRaw)
                        .key(keyRaw)
                        .build());
             BufferedReader leitor = new BufferedReader(new InputStreamReader(rawStream, StandardCharsets.UTF_8))) {

            String cabecalho = leitor.readLine();
            if (cabecalho == null) {
                resultado.append("Arquivo de processos vazio!\n");
                return resultado.toString();
            }

            System.out.println("Cabecalho processos: " + cabecalho);
            resultado.append("Cabecalho processos carregado\n");

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

            String resumo = "Arquivo de processos processado! Linhas lidas: " + linhasProcessos;
            System.out.println(resumo);
            resultado.append(resumo).append("\n");

        } catch (Exception e) {
            String erro = "Erro ao processar arquivo de processos: " + e.getMessage();
            System.out.println(erro);
            e.printStackTrace();
            resultado.append(erro).append("\n");
        }

        return resultado.toString();
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

    private static void salvarArquivosTrusted(S3Client s3, String bucketTrusted, String bucketClient,
                                              List<ServidorArquivo> arquivos, String data) {
        for (ServidorArquivo arquivo : arquivos) {
            salvarArquivoPrincipal(s3, bucketTrusted, arquivo, data);

            if (arquivo.temProcessos()) {
                salvarArquivoProcessosCliente(s3, bucketClient, arquivo, data);
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

    private static void salvarArquivoProcessosCliente(S3Client s3, String bucketClient, ServidorArquivo arquivo, String data) {
        try {
            String empresaFolder = formatarNome(arquivo.getEmpresaNome());
            String servidorFolder = formatarNome(arquivo.getServidorNome());

            String key = String.format("%s/%s/processos/ProcessosUso_%s.csv",
                    empresaFolder, servidorFolder, data);

            String csvProcessos = arquivo.gerarCSVProcessosOrdenado();
            s3.putObject(
                    PutObjectRequest.builder()
                            .bucket(bucketClient)
                            .key(key)
                            .contentType("text/csv")
                            .build(),
                    RequestBody.fromString(csvProcessos)
            );

            System.out.println("Arquivo de processos enviado para cliente: " + key);

        } catch (Exception e) {
            System.out.println("Erro ao enviar arquivo de processos para cliente: " + e.getMessage());
            e.printStackTrace();
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
            System.out.println("Buscando servidor no banco: '" + servidorNome + "'");

            String sql = "SELECT s.id, s.nome, e.razao_social as empresa_nome, e.id as empresa_id, COALESCE(ls.leituras_consecutivas_para_alerta, 3) as leituras_consecutivas_para_alerta FROM servidor s JOIN empresa e ON s.fk_empresa = e.id LEFT JOIN leitura_script ls ON ls.fk_servidor = s.id WHERE s.nome = ? LIMIT 1";

            List<ServidorConfig> resultados = con.query(sql, (rs, rowNum) ->
                    new ServidorConfig(
                            rs.getInt("id"),
                            rs.getString("nome"),
                            rs.getString("empresa_nome"),
                            rs.getInt("empresa_id"),
                            rs.getInt("leituras_consecutivas_para_alerta")
                    ), servidorNome);

            if (!resultados.isEmpty()) {
                System.out.println("Servidor encontrado: " + resultados.get(0).getNome());
                return resultados.get(0);
            } else {
                System.out.println("Servidor NÃO encontrado: " + servidorNome);
                listarTodosServidores(con);
                return null;
            }

        } catch (Exception e) {
            System.out.println("Erro ao buscar servidor '" + servidorNome + "': " + e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    private static void listarTodosServidores(JdbcTemplate con) {
        try {
            String sql = "SELECT id, nome, fk_empresa FROM servidor";
            var servidores = con.queryForList(sql);

            System.out.println("SERVIDORES DISPONÍVEIS NO BANCO:");
            for (var serv : servidores) {
                System.out.printf("ID: %s | Nome: '%s' | Empresa: %s%n",
                        serv.get("id"), serv.get("nome"), serv.get("fk_empresa"));
            }
        } catch (Exception e) {
            System.out.println("Erro ao listar servidores: " + e.getMessage());
        }
    }

    private static ComponenteLimites buscarLimitesServidor(JdbcTemplate con, int servidorId) {
        ComponenteLimites limites = new ComponenteLimites();
        try {
            String sql = "SELECT tc.nome_tipo_componente as nome_tipo_componente, g.nome as gravidade, m.valor as valor FROM metrica m JOIN gravidade g ON m.fk_gravidade = g.id JOIN tipo_componente tc ON m.fk_componenteServidor_tipoComponente = tc.id WHERE m.fk_componenteServidor_servidor = ? ORDER BY tc.nome_tipo_componente, g.id";

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