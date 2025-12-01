package school.sptech;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlertaService {
    private final JdbcTemplate con;

    public AlertaService(JdbcTemplate con) {
        this.con = con;
    }


    public Map<String, Integer> buscarConsolidadoAlertas(Integer idServidor, int ano, Integer mes) {
        Map<String, Integer> contador = new HashMap<>();
        contador.put("Baixo", 0);
        contador.put("Médio", 0);
        contador.put("Alto", 0);

        try {

            StringBuilder sql = new StringBuilder("""
                    SELECT g.nome as gravidade, COUNT(a.id) as total
                    FROM alerta a
                    JOIN gravidade g ON a.fk_gravidade = g.id
                    WHERE a.fk_componenteServidor_servidor = ?
                    AND YEAR(a.inicio) = ?
                    """);

            // Lista para armazenar os parâmetros
            List<Object> params = new java.util.ArrayList<>();
            params.add(idServidor);
            params.add(ano);

            // Adiciona filtro mensal, se o mês for fornecido
            if (mes != null) {
                sql.append(" AND MONTH(a.inicio) = ?");
                params.add(mes);
            }

            sql.append(" GROUP BY g.id, g.nome");

            List<Map<String, Object>> resultados = con.queryForList(sql.toString(), params.toArray());

            for (Map<String, Object> row : resultados) {
                String gravidade = (String) row.get("gravidade");
                int total = ((Number) row.get("total")).intValue();
                contador.put(gravidade, total);
            }

        } catch (Exception e) {
            System.out.printf("Erro ao buscar consolidado (Servidor %d, Ano %d, Mês %s): %s%n",
                    idServidor, ano, (mes == null ? "Todos" : mes), e.getMessage());
        }

        return contador;
    }
}
