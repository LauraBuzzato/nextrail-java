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

    public Integer buscarIdServidor(String nomeServidor) {
        try {
            String sql = "SELECT id FROM servidor WHERE nome = ?";
            System.out.println("Buscando servidor: '" + nomeServidor + "'");

            Integer id = con.queryForObject(sql, Integer.class, nomeServidor);
            System.out.println("Servidor encontrado: '" + nomeServidor + "' -> ID: " + id);
            return id;
        } catch (Exception e) {
            System.out.println("Servidor NÃO encontrado: '" + nomeServidor + "' - Erro: " + e.getMessage());
            listarTodosServidores();
            return null;
        }
    }

    private void listarTodosTiposComponentes() {
        try {
            String sql = "SELECT id, nome_tipo_componente FROM tipo_componente";
            var componentes = con.queryForList(sql);

            System.out.println("=== TODOS OS TIPOS DE COMPONENTES NO BANCO ===");
            for (var comp : componentes) {
                System.out.printf("ID: %s | Nome: '%s'%n",
                        comp.get("id"), comp.get("nome_tipo_componente"));
            }
            System.out.println("=============================================");
        } catch (Exception e) {
            System.out.println("Erro ao listar tipos componentes: " + e.getMessage());
        }
    }

    private void listarTodosServidores() {
        try {
            String sql = "SELECT id, nome, fk_empresa FROM servidor";
            var servidores = con.queryForList(sql);

            System.out.println("=== TODOS OS SERVIDORES NO BANCO ===");
            for (var serv : servidores) {
                System.out.printf("ID: %s | Nome: '%s' | Empresa: %s%n",
                        serv.get("id"), serv.get("nome"), serv.get("fk_empresa"));
            }
            System.out.println("====================================");
        } catch (Exception e) {
            System.out.println("Erro ao listar servidores: " + e.getMessage());
        }
    }
}