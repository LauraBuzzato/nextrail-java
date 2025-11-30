package school.sptech;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponenteModelMapper {

    public static List<Map<String, String>> converterLista(List<ComponenteModel> lista) {

        List<Map<String, String>> saida = new ArrayList<>();

        for (ComponenteModel c : lista) {
            Map<String, String> m = new HashMap<>();

            m.put("cpu", format(c.getCpu()));
            m.put("ram", format(c.getRam()));
            m.put("disco", format(c.getDisco()));
            m.put("empresa", c.getEmpresa());
            m.put("servidor", c.getServidor());
            m.put("timestampCaptura", c.getTimestampCaptura());

            saida.add(m);
        }

        return saida;
    }

    private static String format(Double d) {
        if (d == null) return null;
        return String.format("%.2f", d);
    }
}
