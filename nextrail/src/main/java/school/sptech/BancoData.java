package school.sptech;

import org.springframework.jdbc.core.JdbcTemplate;

public class BancoData {

    Connection connection = new Connection();
    JdbcTemplate con = new JdbcTemplate(connection.getDataSource());

}
