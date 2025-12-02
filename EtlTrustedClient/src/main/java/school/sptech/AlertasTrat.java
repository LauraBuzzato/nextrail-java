package school.sptech;

import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDate;
import java.util.*;

import static school.sptech.MontarKey.extrairNomeServidor;

public class AlertasTrat {


    private static final String TRUSTED_BUCKET = "nextrail-trusted-log";
    private static final String CLIENT_BUCKET  = "nextrail-client-log";

    private static final S3Service s3 = new S3Service();

    public static void processarAlertas() {
        System.out.println("--- INICIANDO PROCESSO DE ALERTAS (ENZO) ---");

        LocalDate hoje = LocalDate.now();
        int anoAtual = hoje.getYear();
        int mesAtual = hoje.getMonthValue();

        Connection connection = new Connection();
        JdbcTemplate con = new JdbcTemplate(connection.getDataSource());


        ServidorId servidorId = new ServidorId(con);
        AlertaService alertaService = new AlertaService(con);

        try {

            ListObjectsV2Response empresas = s3.listarPastas(TRUSTED_BUCKET);

            for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
                String empresa = empresaPrefix.prefix();


                ListObjectsV2Response servidores = s3.listarComPrefixo(TRUSTED_BUCKET, empresa, true);

                for (CommonPrefix servidorPrefix : servidores.commonPrefixes()) {
                    String servidorPath = servidorPrefix.prefix(); // ex: Empresa/Servidor01/
                    String servidorNome = extrairNomeServidor(servidorPath);


                    int idServidor = servidorId.buscarIdPeloNome(servidorNome);

                    if (idServidor == 0) {
                        System.out.println("Alertas: Servidor " + servidorNome + " não encontrado no banco.");
                        continue; // Pula para o próximo
                    }

                    System.out.println("Processando alertas para: " + servidorNome);

                    // --- LÓGICA MENSAL ---
                    Map<String, Integer> consolidadoMensal = alertaService.buscarConsolidadoAlertas(idServidor, anoAtual, mesAtual);

                    Map<String, Integer> componentesMensal = alertaService.buscarComponentes(idServidor, anoAtual, mesAtual);

                    List<Map<String, Object>> historicoMensal = alertaService.buscarHistorico(idServidor, anoAtual, mesAtual);

                    Map<String, Object> graficoMensal = montarDadosGrafico(historicoMensal);

                    Double mttrMensal = alertaService.calcularMTTR(idServidor, anoAtual, mesAtual);

                    Integer slaMensal = alertaService.buscarSla(idServidor);

                    Map<String, Integer> comparacao = alertaService.buscarComparacaoMesAnterior(idServidor, anoAtual, mesAtual);

                    Double pctCrescimento = calcularCrescimento(comparacao.get("qtd_anterior"), comparacao.get("qtd_atual"));

                    Map<String, Object> jsonMensal = criarObjetoJsonAlerta(
                            idServidor, servidorNome, anoAtual, mesAtual, consolidadoMensal,
                            componentesMensal, historicoMensal, graficoMensal, mttrMensal, slaMensal,
                            comparacao.get("qtd_anterior"), pctCrescimento);

                    String chaveMensal = MontarKey.gerarChaveMensalAlerta(servidorPath, hoje);
                    s3.enviarJsonObject(CLIENT_BUCKET, chaveMensal, jsonMensal);
                    System.out.println("✅ Alert Mensal salvo: " + chaveMensal);

                    // --- LÓGICA ANUAL ---
                    Map<String, Integer> consolidadoAnual = alertaService.buscarConsolidadoAlertas(idServidor, anoAtual, null);

                    Map<String, Integer> componentesAnual = alertaService.buscarComponentes(idServidor, anoAtual, null);

                    List<Map<String, Object>> historicoAnual = alertaService.buscarHistorico(idServidor, anoAtual, null);

                    Map<String, Object> graficoAnual = montarDadosGrafico(historicoAnual);

                    Double mttrAnual = alertaService.calcularMTTR(idServidor, anoAtual, null);

                    Integer slaAnual = alertaService.buscarSla(idServidor);

                    Map<String, Integer> comparacaoAnual = alertaService.buscarComparacaoAnoAnterior(idServidor, anoAtual);

                    Double pctCrescimentoAnual = calcularCrescimento(comparacaoAnual.get("qtd_anterior"), comparacaoAnual.get("qtd_atual"));

                    Map<String, Object> jsonAnual = criarObjetoJsonAlerta(
                            idServidor, servidorNome, anoAtual, null, consolidadoAnual,
                            componentesAnual, historicoAnual, graficoAnual, mttrAnual, slaAnual,
                            comparacaoAnual.get("qtd_anterior"), pctCrescimentoAnual);

                    String chaveAnual = MontarKey.gerarChaveAnualAlerta(servidorPath, hoje);
                    s3.enviarJsonObject(CLIENT_BUCKET, chaveAnual, jsonAnual);
                    System.out.println("✅ Alert Anual salvo: " + chaveAnual);
                }
            }
        } catch (Exception e) {
            System.out.println("ERRO FATAL EM ALERTAS TRAT: " + e.getMessage());
            e.printStackTrace();
        }
    }



    //a classe não é instanciada então é td static
    private static Double calcularCrescimento(Integer anterior, Integer atual) {
        if (anterior > 0) return ((double) (atual - anterior) / anterior) * 100;
        else if (atual > 0) return 100.0;
        return 0.0;
    }

    private static Map<String, Object> criarObjetoJsonAlerta(int idServidor, String nomeServidor,
                                                             int ano, Integer mes,
                                                             Map<String, Integer> gravidade, Map<String, Integer> componentes,
                                                             List<Map<String, Object>> historico, Map<String, Object> graficoFrequencia,
                                                             Double mttr, Integer sla, Integer qtdAnterior, Double pctCrescimento) {

        Map<String, Object> json = new LinkedHashMap<>();
        json.put("servidor_id", idServidor);
        json.put("servidor_nome", nomeServidor);
        json.put("ano_referencia", ano);

        if (mes != null) {
            json.put("mes_referencia", mes);
            json.put("alertas_qtd_mes_anterior", qtdAnterior);
        } else {
            json.put("alertas_qtd_ano_anterior", qtdAnterior);
        }

        double pctFormatada = Math.round(pctCrescimento * 10.0) / 10.0;
        json.put("alertas_pct_crescimento", pctFormatada);
        json.put("total_alertas_baixo", gravidade.get("Baixo"));
        json.put("total_alertas_medio", gravidade.get("Médio"));
        json.put("total_alertas_alto", gravidade.get("Alto"));
        json.put("total_alertas_cpu", componentes.get("Cpu"));
        json.put("total_alertas_ram", componentes.get("Ram"));
        json.put("total_alertas_disco", componentes.get("Disco"));
        json.put("mttr", Math.round(mttr));
        json.put("sla", sla);
        json.put("historico_diario", historico);
        json.put("grafico_linha", graficoFrequencia);

        return json;
    }

    private static Map<String, Object> montarDadosGrafico(List<Map<String, Object>> historicoSql) {
        Map<String, Map<String, Integer>> agrupado = new LinkedHashMap<>();

        for (Map<String, Object> linha : historicoSql) {
            String periodo = (String) linha.get("periodo");
            String comp = (String) linha.get("componente");
            int qtd = ((Number) linha.get("total")).intValue();

            if (!agrupado.containsKey(periodo)) {
                agrupado.put(periodo, new HashMap<>());
            }
            agrupado.get(periodo).put(comp, qtd);
        }

        List<String> labels = new ArrayList<>();
        List<Integer> dadosCpu = new ArrayList<>();
        List<Integer> dadosRam = new ArrayList<>();
        List<Integer> dadosDisco = new ArrayList<>();

        for (String dataChave : agrupado.keySet()) {
            String labelFormatada;
            String[] partes = dataChave.split("-");
            if (partes.length == 3) {
                labelFormatada = partes[2] + "/" + partes[1];
            } else {
                labelFormatada = "Mês " + partes[1];
            }
            labels.add(labelFormatada);

            Map<String, Integer> valores = agrupado.get(dataChave);
            dadosCpu.add(valores.getOrDefault("Cpu", 0));
            dadosRam.add(valores.getOrDefault("Ram", 0));
            dadosDisco.add(valores.getOrDefault("Disco", 0));
        }

        Map<String, Object> resultado = new HashMap<>();
        resultado.put("labels", labels);
        resultado.put("cpu", dadosCpu);
        resultado.put("ram", dadosRam);
        resultado.put("disco", dadosDisco);

        return resultado;
    }
}