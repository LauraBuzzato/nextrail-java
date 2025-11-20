package school.sptech;

import java.time.LocalDate;

public class MontarKey {

    public static String gerarMensalKey(String csvKey, LocalDate hoje) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        String nomeArquivo = String.format("coleta_%d-%02d.json",
                hoje.getYear(), hoje.getMonthValue());

        return String.format("dadosDashComponentes/%s/%s/%s",
                empresa, servidor, nomeArquivo);
    }

    public static String gerarAnualKey(String csvKey, LocalDate hoje) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        String nomeArquivo = String.format("coleta_%d.json", hoje.getYear());

        return String.format("dadosDashComponentes/%s/%s/%s",
                empresa, servidor, nomeArquivo);
    }
}
