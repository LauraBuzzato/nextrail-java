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


    public void gerarRelatorioTeste() {
        System.out.println("GERANDO RELATÓRIO DE TESTE");
        gerarEEnviarRelatorio();
        System.out.println("Relatório de teste enviado para Slack/Jira!");
    }


    private void gerarEEnviarRelatorio() {
        System.out.println("Gerando relatório consolidado...");


        String sqlServidores = """
                SELECT DISTINCT s.id as servidorId, s.nome as servidorNome, e.razao_social as empresaNome
                FROM alerta a
                JOIN servidor s ON a.fk_componenteServidor_servidor = s.id
                JOIN empresa e ON s.fk_empresa = e.id
                WHERE a.inicio >= NOW() - INTERVAL 1 MONTH
                """;

        List<Map<String, Object>> servidores = con.queryForList(sqlServidores);

        if (servidores.isEmpty()) {
            System.out.println("Nenhum alerta registrado no período.");
            notificador.enviarRelatorioConsolidado(
                    "Relatório de Alertas - Sem Alertas",
                    "RELATÓRIO DE ALERTAS\nNenhum alerta registrado na última hora."
            );
            return;
        }

        for (Map<String, Object> servidor : servidores) {
            Integer servidorId = ((Number) servidor.get("servidorId")).intValue();
            String servidorNome = (String) servidor.get("servidorNome");
            String empresaNome = (String) servidor.get("empresaNome");


            Map<String, Integer> contadorGravidade = contarAlertasPorGravidade(servidorId);

            Set<String> componentesAlto = buscarComponentesComAlertaAlto(servidorId);


            StringBuilder mensagem = new StringBuilder();
            mensagem.append("*RELATÓRIO DE ALERTAS - ÚLTIMA HORA*\n");
            mensagem.append("Empresa: ").append(empresaNome).append("\n");
            mensagem.append("Servidor: ").append(servidorNome).append("\n");
            mensagem.append("----------------------------------------\n");
            mensagem.append("RESUMO DE ALERTAS:\n");
            mensagem.append("• Baixo: ").append(contadorGravidade.get("Baixo")).append("\n");
            mensagem.append("• Médio: ").append(contadorGravidade.get("Médio")).append("\n");
            mensagem.append("• Alto: ").append(contadorGravidade.get("Alto")).append("\n");

            if (!componentesAlto.isEmpty()) {
                mensagem.append("COMPONENTES COM ALERTA ALTO: ").append(String.join(", ", componentesAlto)).append("\n");
            }

            mensagem.append("----------------------------------------\n");
            mensagem.append("Período: Última hora\n");

            System.out.println("RELATÓRIO:");
            System.out.println(mensagem);


            notificador.enviarRelatorioConsolidado(
                    "Relatório de Alertas - " + servidorNome,
                    mensagem.toString()
            );
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
                    AND a.inicio >= NOW() - INTERVAL 1 MONTH
                    GROUP BY g.id, g.nome
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            for (Map<String, Object> row : resultados) {
                String gravidade = (String) row.get("gravidade");
                int total = ((Number) row.get("total")).intValue();
                contador.put(gravidade, total);
            }

            System.out.println("Contagem alertas - Alto: " + contador.get("Alto") +
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
                    AND a.inicio >= NOW() - INTERVAL 1 MONTH
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            for (Map<String, Object> row : resultados) {
                componentes.add((String) row.get("componente"));
            }

            System.out.println("Componentes com alerta ALTO: " + componentes);

        } catch (Exception e) {
            System.out.println("Erro ao buscar componentes com alerta alto: " + e.getMessage());
        }

        return componentes;
    }


    public void debugDadosAlertas() {
        System.out.println("=== DEBUG: DADOS DE ALERTAS NO BANCO ===");

        String sqlDebug = """
                SELECT 
                    s.nome as servidor,
                    tc.nome_tipo_componente as componente,
                    g.nome as gravidade,
                    COUNT(a.id) as total_alertas,
                    a.inicio
                FROM alerta a
                JOIN servidor s ON a.fk_componenteServidor_servidor = s.id
                JOIN tipo_componente tc ON a.fk_componenteServidor_tipoComponente = tc.id
                JOIN gravidade g ON a.fk_gravidade = g.id
                WHERE a.inicio >= NOW() - INTERVAL 1 MONTH
                GROUP BY s.id, s.nome, tc.id, tc.nome_tipo_componente, g.id, g.nome, a.inicio
                ORDER BY a.inicio DESC
                LIMIT 10
                """;

        List<Map<String, Object>> debugResults = con.queryForList(sqlDebug);

        System.out.println("ÚLTIMOS 10 ALERTAS:");
        for (Map<String, Object> row : debugResults) {
            System.out.println("Servidor: " + row.get("servidor") +
                    " | Componente: " + row.get("componente") +
                    " | Gravidade: " + row.get("gravidade") +
                    " | Alertas: " + row.get("total_alertas") +
                    " | Início: " + row.get("inicio"));
        }

        if (debugResults.isEmpty()) {
            System.out.println("Nenhum alerta encontrado na última hora!");
        }
    }
}
