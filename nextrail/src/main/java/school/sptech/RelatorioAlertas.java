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
        relatorioConsolidado.append("RELATÓRIO DE PROCESSAMENTO\n");

        if (servidoresProcessados.isEmpty()) {
            System.out.println("Nenhum servidor processado para gerar relatório.");
            relatorioConsolidado.append("Nenhum servidor processado no CSV.\n");
            notificador.enviarRelatorioConsolidado("Relatório ETL - Nenhum servidor", relatorioConsolidado.toString());
            return relatorioConsolidado.toString();
        }

        String placeholders = String.join(",", Collections.nCopies(servidoresProcessados.size(), "?"));

        String sqlServidores = """
                SELECT DISTINCT s.id as servidorId, s.nome as servidorNome, e.razao_social as empresaNome
                FROM servidor s
                JOIN empresa e ON s.fk_empresa = e.id
                WHERE s.nome IN (""" + placeholders + ")";

        List<Map<String, Object>> servidores = con.queryForList(sqlServidores, servidoresProcessados.toArray());

        relatorioConsolidado.append("Servidores lidos do CSV: ").append(String.join(", ", servidoresProcessados)).append("\n");
        relatorioConsolidado.append("Total de servidores processados: ").append(servidoresProcessados.size()).append("\n");
        relatorioConsolidado.append("Servidores encontrados no banco: ").append(servidores.size()).append("\n");
        relatorioConsolidado.append("------------------------------------------\n");

        if (servidores.isEmpty()) {
            relatorioConsolidado.append("Nenhum dos servidores processados foi encontrado no banco de dados.\n");
            relatorioConsolidado.append("Verifique se os nomes no CSV correspondem aos nomes no banco.\n");
            notificador.enviarRelatorioConsolidado("Relatório ETL - Nenhum servidor no banco", relatorioConsolidado.toString());

        } else {
            List<TicketServidor> ticketsServidores = new ArrayList<>();
            int servidoresComAlertas = 0;

            for (Map<String, Object> servidor : servidores) {
                Integer servidorId = ((Number) servidor.get("servidorId")).intValue();
                String servidorNome = (String) servidor.get("servidorNome");
                String empresaNome = (String) servidor.get("empresaNome");

                Map<String, Integer> contadorGravidade = contarAlertasPorGravidade(servidorId);
                int totalAlertas = contadorGravidade.get("Baixo") + contadorGravidade.get("Médio") + contadorGravidade.get("Alto");

                relatorioConsolidado.append("Empresa: ").append(empresaNome).append("\n");
                relatorioConsolidado.append("Servidor: ").append(servidorNome).append("\n");
                relatorioConsolidado.append("Alertas na última hora - Baixo: ").append(contadorGravidade.get("Baixo"))
                        .append(" | Médio: ").append(contadorGravidade.get("Médio"))
                        .append(" | Alto: ").append(contadorGravidade.get("Alto")).append("\n");

                if (totalAlertas > 0) {
                    servidoresComAlertas++;
                    Set<String> componentesAlto = buscarComponentesComAlertaAlto(servidorId);

                    if (!componentesAlto.isEmpty()) {
                        relatorioConsolidado.append("Componentes com ALERTA ALTO: ").append(String.join(", ", componentesAlto)).append("\n");
                    }

                    StringBuilder descricaoTicket = new StringBuilder();
                    descricaoTicket.append("=== DETALHES DO SERVIDOR ===\n");
                    descricaoTicket.append("Empresa: ").append(empresaNome).append("\n");
                    descricaoTicket.append("Servidor: ").append(servidorNome).append("\n");
                    descricaoAlertasPorGravidade(servidorId, descricaoTicket);
                    descricaoTicket.append("\nData/hora do processamento: ").append(new Date()).append("\n");

                    String tituloTicket = String.format("%s - %s - Resumo de coleta",
                            empresaNome, servidorNome);

                    ticketsServidores.add(new TicketServidor(tituloTicket, descricaoTicket.toString()));

                    relatorioConsolidado.append(">> TICKET JIRA CRIADO para este servidor\n");
                } else {
                    relatorioConsolidado.append(">> Nenhum alerta - Ticket Jira NÃO criado\n");
                }
                relatorioConsolidado.append("----------------------------------------\n");
            }

            relatorioConsolidado.append("RESUMO GERAL:\n");
            relatorioConsolidado.append("Total de servidores processados: ").append(servidores.size()).append("\n");
            relatorioConsolidado.append("Servidores com alertas: ").append(servidoresComAlertas).append("\n");
            relatorioConsolidado.append("Servidores sem alertas: ").append(servidores.size() - servidoresComAlertas).append("\n");
            relatorioConsolidado.append("Total de tickets Jira criados: ").append(ticketsServidores.size()).append("\n");

            if (!ticketsServidores.isEmpty()) {
                System.out.println("\nCRIANDO TICKETS JIRA INDIVIDUAIS PARA SERVIDORES COM ALERTAS:");
                for (TicketServidor ticket : ticketsServidores) {
                    System.out.println("Criando ticket para: " + ticket.getTitulo());
                    notificador.criarJiraTicketIndividual(ticket.getTitulo(), ticket.getDescricao());
                }
            } else {
                System.out.println("Nenhum servidor com alertas - Nenhum ticket Jira será criado.");
            }
        }

        relatorioConsolidado.append("Período analisado: Última hora\n");
        relatorioConsolidado.append("Data/hora do relatório: ").append(new Date()).append("\n");

        System.out.println("RELATÓRIO CONSOLIDADO:");
        System.out.println(relatorioConsolidado);

        notificador.enviarRelatorioConsolidado("Relatório ETL Consolidado", relatorioConsolidado.toString());

        return relatorioConsolidado.toString();
    }

    private void descricaoAlertasPorGravidade(Integer servidorId, StringBuilder descricao) {
        try {
            String sql = """
                    SELECT g.nome as gravidade, tc.nome_tipo_componente as componente, 
                           COUNT(a.id) as total, MAX(a.inicio) as ultimo_alerta
                    FROM alerta a
                    JOIN gravidade g ON a.fk_gravidade = g.id
                    JOIN tipo_componente tc ON a.fk_componenteServidor_tipoComponente = tc.id
                    WHERE a.fk_componenteServidor_servidor = ?
                    AND a.inicio >= NOW() - INTERVAL 1 HOUR
                    GROUP BY g.id, g.nome, tc.nome_tipo_componente
                    ORDER BY g.id DESC, tc.nome_tipo_componente
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            if (resultados.isEmpty()) {
                descricao.append("Nenhum alerta específico encontrado na última hora.\n");
                return;
            }

            descricao.append("Alertas na última hora:\n");
            for (Map<String, Object> row : resultados) {
                String gravidade = (String) row.get("gravidade");
                String componente = (String) row.get("componente");
                int total = ((Number) row.get("total")).intValue();
                descricao.append(String.format("- %s (%s): %d alerta(s)\n", componente, gravidade, total));
            }
        } catch (Exception e) {
            descricao.append("Erro ao obter detalhes dos alertas: ").append(e.getMessage()).append("\n");
        }
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

        } catch (Exception e) {
            System.out.println("Erro ao contar alertas para servidor " + servidorId + ": " + e.getMessage());
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
                    AND a.fk_gravidade = 3
                    AND a.inicio >= NOW() - INTERVAL 1 HOUR
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            for (Map<String, Object> row : resultados) {
                componentes.add((String) row.get("componente"));
            }

        } catch (Exception e) {
            System.out.println("Erro ao buscar componentes com alerta alto: " + e.getMessage());
        }

        return componentes;
    }

    private static class TicketServidor {
        private String titulo;
        private String descricao;

        public TicketServidor(String titulo, String descricao) {
            this.titulo = titulo;
            this.descricao = descricao;
        }

        public String getTitulo() {
            return titulo;
        }

        public String getDescricao() {
            return descricao;
        }
    }
}