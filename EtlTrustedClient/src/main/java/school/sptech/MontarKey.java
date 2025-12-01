package school.sptech;

import java.time.LocalDate;

public class MontarKey {

    public static String gerarMensalKey(String csvKey, LocalDate data) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("%s/%s/dadosDashComponentes/mensal_%d-%02d.json",
                empresa, servidor,
                data.getYear(), data.getMonthValue());
    }

    public static String gerarAnualKey(String csvKey, LocalDate data) {

        String[] partes = csvKey.split("/");

        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("%s/%s/dadosDashComponentes/anual_%d.json",
                empresa, servidor,
                data.getYear());
    }



    //Enzo
    public static String extrairNomeServidor(String servidorPath) {
        String[] partes = servidorPath.split("/");

        // Verifica se a divisão resultou na (empresa e servidor).

        if (partes.length > 1) {
            return partes[1];
        } else {
            return "";
        }
    }


    //Chave mensal
    public static String gerarChaveMensalAlerta(String servidorPath, LocalDate data) {
        String[] partes = servidorPath.split("/");
        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("dadosDashAlertas/%s/%s/mensal_%d-%02d.json",
                empresa, servidor,
                data.getYear(), data.getMonthValue());
    }


    public static String gerarChaveAnualAlerta(String servidorPath, LocalDate data) {
        String[] partes = servidorPath.split("/");
        String empresa  = partes[0];
        String servidor = partes[1];

        return String.format("dadosDashAlertas/%s/%s/anual_%d.json",
                empresa, servidor,
                data.getYear());
    }
}