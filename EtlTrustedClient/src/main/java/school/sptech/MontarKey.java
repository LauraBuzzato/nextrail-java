package school.sptech;

import java.time.LocalDate;

public class MontarKey {

    public static String gerarMensalKey(String csvKey, LocalDate data) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("dadosDashComponentes/%s/%s/mensal_%d-%02d.json",
                empresa, servidor,
                data.getYear(), data.getMonthValue());
    }

    public static String gerarAnualKey(String csvKey, LocalDate data) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("dadosDashComponentes/%s/%s/anual_%d.json",
                empresa, servidor,
                data.getYear());
    }
}
