package school.sptech;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

public class ServidorId {

    private final JdbcTemplate con;

    public ServidorId(JdbcTemplate con) {
        this.con = con;
    }

    public int buscarIdPeloNome(String servidorNome) {
        if (servidorNome == null || servidorNome.trim().isEmpty()) {
            return 0;
        }

        try {
            String sql = """
                    SELECT id 
                    FROM servidor 
                    WHERE nome = ?
                    """;

            List<Map<String, Object>> resultados = con.queryForList(sql, servidorNome.trim());

            if (!resultados.isEmpty()) {
                Object id = resultados.get(0).get("id");
                if (id instanceof Number) {
                    return ((Number) id).intValue();
                }
            }

        } catch (Exception e) {
            System.out.printf("Erro ao buscar ID do servidor '%s': %s%n", servidorNome, e.getMessage());

        }

        return 0;
    }

}
