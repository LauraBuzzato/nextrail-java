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

}
