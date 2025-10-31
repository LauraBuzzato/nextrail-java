package school.sptech;

import org.apache.commons.dbcp2.BasicDataSource;

public class Connection {

    private BasicDataSource dataSource;

    public Connection() {
        dataSource = new BasicDataSource();

        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");

        dataSource.setUrl("jdbc:mysql://52.54.194.9:3306/nextrail?useSSL=false&serverTimezone=UTC");

        // USUÁRIO E SENHA DO MYSQL
        dataSource.setUsername(Credenciais.getMysqlUser());
        dataSource.setPassword(Credenciais.getMysqlPassword());

        dataSource.setInitialSize(5);
        dataSource.setMaxTotal(10);
    }

    public BasicDataSource getDataSource() {
        return dataSource;
    }

}
