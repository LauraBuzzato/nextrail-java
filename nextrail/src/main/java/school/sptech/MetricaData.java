package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetricaData {

    private final JdbcTemplate con;

    public MetricaData(JdbcTemplate con) {
        this.con = con;
    }

    // Buscar limites por servidor e tipo de componente
    public Map<String, Map<String, Double>> buscarLimitesPorServidorTipoComponente(Integer servidorId, Integer tipoComponenteId) {
        String sql = """
                SELECT m.nome as nomeMetrica, g.nome as gravidade, m.valor
                FROM metrica m
                JOIN gravidade g ON m.fk_gravidade = g.id
                WHERE m.fk_componenteServidor_servidor = ? 
                AND m.fk_componenteServidor_tipoComponente = ?
                """;

        List<Map<String, Object>> resultados = con.queryForList(sql, servidorId, tipoComponenteId);

        Map<String, Map<String, Double>> limites = new HashMap<>();

        for (Map<String, Object> row : resultados) {
            String nomeMetrica = (String) row.get("nomeMetrica");
            String gravidade = (String) row.get("gravidade");
            Double valor = ((Number) row.get("valor")).doubleValue();

            limites.computeIfAbsent(nomeMetrica, k -> new HashMap<>()).put(gravidade, valor);
        }

        return limites;
    }

    // Buscar todos os limites de um servidor
    public Map<Integer, Map<String, Map<String, Double>>> buscarTodosLimitesServidor(Integer servidorId) {
        String sql = """
                SELECT m.fk_componenteServidor_tipoComponente as tipoComponenteId, 
                       m.nome as nomeMetrica, 
                       g.nome as gravidade, 
                       m.valor
                FROM metrica m
                JOIN gravidade g ON m.fk_gravidade = g.id
                WHERE m.fk_componenteServidor_servidor = ?
                """;

        List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

        Map<Integer, Map<String, Map<String, Double>>> limites = new HashMap<>();

        for (Map<String, Object> row : resultados) {
            Integer tipoComponenteId = ((Number) row.get("tipoComponenteId")).intValue();
            String nomeMetrica = (String) row.get("nomeMetrica");
            String gravidade = (String) row.get("gravidade");
            Double valor = ((Number) row.get("valor")).doubleValue();

            limites.computeIfAbsent(tipoComponenteId, k -> new HashMap<>())
                    .computeIfAbsent(nomeMetrica, k -> new HashMap<>())
                    .put(gravidade, valor);
        }

        return limites;
    }

    // Debug para verificar métricas existentes
    public void debugMetricasExistentes(Integer servidorId) {
        try {
            String sql = """
                    SELECT 
                        s.nome as servidor,
                        tc.nome_tipo_componente as tipoComponente,
                        m.nome as metrica, 
                        g.nome as gravidade, 
                        m.valor
                    FROM metrica m
                    JOIN servidor s ON m.fk_componenteServidor_servidor = s.id
                    JOIN tipo_componente tc ON m.fk_componenteServidor_tipoComponente = tc.id
                    JOIN gravidade g ON m.fk_gravidade = g.id
                    WHERE m.fk_componenteServidor_servidor = ?
                    ORDER BY tc.nome_tipo_componente, m.nome, g.nome
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorId);

            System.out.println("=== MÉTRICAS EXISTENTES PARA SERVIDOR ID: " + servidorId + " ===");
            for (Map<String, Object> row : resultados) {
                System.out.printf("Servidor: %s | Tipo: %s | Métrica: %s | Gravidade: %s | Valor: %.2f%n",
                        row.get("servidor"), row.get("tipoComponente"), row.get("metrica"),
                        row.get("gravidade"), row.get("valor"));
            }
            System.out.println("==========================================================");

        } catch (Exception e) {
            System.out.println("Erro no debug: " + e.getMessage());
        }
    }
}