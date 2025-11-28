package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;

import java.util.*;

public class RelatorioAlertas {

    private final JdbcTemplate con;
    private final Notificador notificador;

    public RelatorioAlertas(JdbcTemplate con, Notificador notificador) {
        this.con = con;
        this.notificador = notificador;
    }

    public String gerarRelatorioAposProcessamento(List<String> servidoresProcessados) {
        System.out.println("GERANDO RELATÓRIO APÓS PROCESSAMENTO DO CSV");
        return gerarEEnviarRelatorioFiltrado(servidoresProcessados);
    }

    private String gerarEEnviarRelatorioFiltrado(List<String> servidoresProcessados) {
        StringBuilder relatorioConsolidado = new StringBuilder();

        System.out.println("Gerando relatório filtrado para servidores processados: " + servidoresProcessados);
        relatorioConsolidado.append("=== RELATÓRIO DE PROCESSAMENTO ===\n");

        if (servidoresProcessados.isEmpty()) {
            System.out.println("Nenhum servidor processado para gerar relatório.");
            relatorioConsolidado.append("Nenhum servidor processado no CSV.\n");

            notificador.enviarRelatorioConsolidado(
                    "Relatório - Processamento CSV",
                    "Nenhum servidor processado no CSV."
            );
            return relatorioConsolidado.toString();
        }

        String placeholders = String.join(",", Collections.nCopies(servidoresProcessados.size(), "?"));

        String sqlServidores = """
                SELECT DISTINCT s.id as servidorId, s.nome as servidorNome, e.razao_social as empresaNome
                FROM alerta a
                JOIN servidor s ON a.fk_componenteServidor_servidor = s.id
                JOIN empresa e ON s.fk_empresa = e.id
                WHERE s.nome IN (""" + placeholders + ") " + """
                AND a.inicio >= NOW() - INTERVAL 1 HOUR
                """;

        List<Map<String, Object>> servidores = con.queryForList(sqlServidores, servidoresProcessados.toArray());

        relatorioConsolidado.append("Servidores lidos do CSV: ").append(String.join(", ", servidoresProcessados)).append("\n");
        relatorioConsolidado.append("Total de servidores processados: ").append(servidoresProcessados.size()).append("\n");
        relatorioConsolidado.append("------------------------------------------\n");

        if (servidores.isEmpty()) {
            relatorioConsolidado.append("Nenhum alerta gerado durante o processamento para os servidores listados.\n");
            relatorioConsolidado.append("Possíveis causas:\n");
            relatorioConsolidado.append("- Os servidores não existem no banco de dados\n");
            relatorioConsolidado.append("- Não houve alertas na última hora\n");
            relatorioConsolidado.append("- Os nomes no CSV não correspondem aos nomes no banco\n");
        } else {
            relatorioConsolidado.append("Servidores com alertas na última hora: ").append(servidores.size()).append("\n");
            relatorioConsolidado.append("----------------------------------------\n");

            for (Map<String, Object> servidor : servidores) {
                Integer servidorId = ((Number) servidor.get("servidorId")).intValue();
                String servidorNome = (String) servidor.get("servidorNome");
                String empresaNome = (String) servidor.get("empresaNome");

                Map<String, Integer> contadorGravidade = contarAlertasPorGravidade(servidorId);
                Set<String> componentesAlto = buscarComponentesComAlertaAlto(servidorId);

                relatorioConsolidado.append("Empresa: ").append(empresaNome).append("\n");
                relatorioConsolidado.append("Servidor: ").append(servidorNome).append("\n");
                relatorioConsolidado.append("Alertas - Baixo: ").append(contadorGravidade.get("Baixo"))
                        .append(" | Médio: ").append(contadorGravidade.get("Médio"))
                        .append(" | Alto: ").append(contadorGravidade.get("Alto")).append("\n");

                if (!componentesAlto.isEmpty()) {
                    relatorioConsolidado.append("Componentes com ALERTA ALTO: ").append(String.join(", ", componentesAlto)).append("\n");
                } else {
                    relatorioConsolidado.append("Nenhum componente com alerta alto\n");
                }
                relatorioConsolidado.append("----------------------------------------\n");
            }


            int totalAlertas = servidores.stream()
                    .mapToInt(servidor -> {
                        Integer servidorId = ((Number) servidor.get("servidorId")).intValue();
                        Map<String, Integer> contador = contarAlertasPorGravidade(servidorId);
                        return contador.get("Baixo") + contador.get("Médio") + contador.get("Alto");
                    })
                    .sum();

            relatorioConsolidado.append("RESUMO GERAL:\n");
            relatorioConsolidado.append("Total de servidores com alertas: ").append(servidores.size()).append("\n");
            relatorioConsolidado.append("Total de alertas gerados: ").append(totalAlertas).append("\n");
        }

        relatorioConsolidado.append("Período analisado: Última hora\n");
        relatorioConsolidado.append("Data/hora do relatório: ").append(new Date()).append("\n");

        System.out.println("RELATÓRIO CONSOLIDADO:");
        System.out.println(relatorioConsolidado);

        notificador.enviarRelatorioConsolidado(
                "Relatório - Processamento CSV - " + servidoresProcessados.size() + " servidores",
                relatorioConsolidado.toString()
        );

        return relatorioConsolidado.toString();
    }

    private Map<String, Integer> contarAlertasPorGravidade(Integer servidorId) {
        Map<String, Integer> contador = new HashMap<>();
        contador.put("Baixo", 0);
        contador.put("Médio", 0);
        contador.put("Alto", 0);

        try {
            String sql = """
                    SELECT g.nome as gravidade, COUNT(a.id) as total
                    FROM alerta a
                    JOIN gravidade g ON a.fk_gravidade = g.id
                    WHERE a.fk_componenteServidor_servidor = ?
                    AND a.inicio >= NOW() - INTERVAL 1 HOUR
                    GROUP BY g.id, g.nome
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            for (Map<String, Object> row : resultados) {
                String gravidade = (String) row.get("gravidade");
                int total = ((Number) row.get("total")).intValue();
                contador.put(gravidade, total);
            }

            System.out.println("Contagem alertas - Servidor " + servidorId +
                    ": Alto: " + contador.get("Alto") +
                    ", Médio: " + contador.get("Médio") +
                    ", Baixo: " + contador.get("Baixo"));

        } catch (Exception e) {
            System.out.println("Erro ao contar alertas: " + e.getMessage());
            e.printStackTrace();
        }

        return contador;
    }

    private Set<String> buscarComponentesComAlertaAlto(Integer servidorId) {
        Set<String> componentes = new HashSet<>();

        try {
            String sql = """
                    SELECT DISTINCT tc.nome_tipo_componente AS componente
                    FROM alerta a
                    JOIN tipo_componente tc ON a.fk_componenteServidor_tipoComponente = tc.id
                    WHERE a.fk_componenteServidor_servidor = ?
                    AND a.fk_gravidade = 3  -- Alto
                    AND a.inicio >= NOW() - INTERVAL 1 HOUR
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            for (Map<String, Object> row : resultados) {
                componentes.add((String) row.get("componente"));
            }

            System.out.println("Componentes com alerta ALTO - Servidor " + servidorId + ": " + componentes);

        } catch (Exception e) {
            System.out.println("Erro ao buscar componentes com alerta alto: " + e.getMessage());
        }

        return componentes;
    }
}