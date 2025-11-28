package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDateTime;
import java.sql.Timestamp;

public class AlertaInsert {

    private JdbcTemplate con;

    public AlertaInsert(JdbcTemplate con) {
        this.con = con;
    }

   
    public void inserirAlerta(Integer servidorId, Integer tipoComponenteId, Integer gravidade, String tempoInicio) {
        String sql = """
                INSERT INTO alerta (fk_componenteServidor_servidor, fk_componenteServidor_tipoComponente, fk_gravidade, inicio)
                VALUES (?, ?, ?, ?)
                """;
        con.update(sql, servidorId, tipoComponenteId, gravidade, tempoInicio );
        System.out.println("Alerta inserido - Servidor: " + servidorId + ", Tipo Componente: " + tipoComponenteId + ", Gravidade: " + gravidade);
    }

  
    public Integer buscarIdTipoComponente(String nomeTipoComponente) {
        try {
            String sql = "SELECT id FROM tipo_componente WHERE nome_tipo_componente = ?";
            System.out.println("Buscando tipo componente: '" + nomeTipoComponente + "'");

            Integer id = con.queryForObject(sql, Integer.class, nomeTipoComponente);
            System.out.println("Tipo componente encontrado: '" + nomeTipoComponente + "' -> ID: " + id);
            return id;
        } catch (Exception e) {
            System.out.println("Tipo componente NÃO encontrado: '" + nomeTipoComponente + "' - Erro: " + e.getMessage());
            listarTodosTiposComponentes();
            return null;
        }
    }

    private void listarTodosTiposComponentes() {
        try {
            String sql = "SELECT id, nome_tipo_componente FROM tipo_componente";
            var componentes = con.queryForList(sql);

            System.out.println("TODOS OS TIPOS DE COMPONENTES NO BANCO");
            for (var comp : componentes) {
                System.out.printf("ID: %s | Nome: '%s'%n",
                        comp.get("id"), comp.get("nome_tipo_componente"));
            }
            System.out.println("-----------------------------------------");
        } catch (Exception e) {
            System.out.println("Erro ao listar tipos componentes: " + e.getMessage());
        }
    }

}