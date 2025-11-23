package school.sptech;

import java.util.*;

public class ServidorArquivo {
    private String empresaNome;
    private String servidorNome;
    private StringBuilder conteudoCSV;
    private boolean cabecalhoEscrito = false;

    private StringBuilder conteudoProcessosCSV;
    private boolean cabecalhoProcessosEscrito = false;
    private List<Processo> processosList;

    public ServidorArquivo(String empresaNome, String servidorNome) {
        this.empresaNome = empresaNome;
        this.servidorNome = servidorNome;
        this.conteudoCSV = new StringBuilder();
        this.conteudoProcessosCSV = new StringBuilder();
        this.processosList = new ArrayList<>();
    }

    public void adicionarLinha(String cabecalho, String linha) {
        if (!cabecalhoEscrito) {
            conteudoCSV.append(cabecalho).append("\n");
            cabecalhoEscrito = true;
        }
        conteudoCSV.append(linha).append("\n");
    }

    public void adicionarLinhaProcessos(String cabecalho, String linha) {
        if (!cabecalhoProcessosEscrito) {
            conteudoProcessosCSV.append(cabecalho).append("\n");
            cabecalhoProcessosEscrito = true;
        }
        conteudoProcessosCSV.append(linha).append("\n");
    }

    public void adicionarProcesso(Processo processo) {
        processosList.add(processo);
    }

    public void ordenarProcessos() {
        int n = processosList.size();
        for (int i = 0; i < n - 1; i++) {
            for (int j = 0; j < n - i - 1; j++) {
                if (processosList.get(j).getUsoMemoria() < processosList.get(j + 1).getUsoMemoria()) {
                    Processo temp = processosList.get(j);
                    processosList.set(j, processosList.get(j + 1));
                    processosList.set(j + 1, temp);
                }
            }
        }
    }

    public String gerarCSVProcessosOrdenado() {
        StringBuilder csvFinal = new StringBuilder();

        if (!cabecalhoProcessosEscrito) {
            csvFinal.append("id;servidor;timestamp;NOME;USO_MEMORIA (MB)\n");
        } else {
            csvFinal.append(conteudoProcessosCSV.toString());
        }

        ordenarProcessos();

        for (Processo processo : processosList) {
            String linha = String.format("%d;%s;%s;%s;%.1f",
                    processo.getId(),
                    processo.getServidor(),
                    processo.getTimestamp(),
                    processo.getNome(),
                    processo.getUsoMemoria());
            csvFinal.append(linha).append("\n");
        }

        return csvFinal.toString();
    }

    public void carregarConteudoExistente(String conteudo) {
        if (conteudo != null && !conteudo.isEmpty()) {
            this.conteudoCSV = new StringBuilder(conteudo);
            this.cabecalhoEscrito = true;
        }
    }

    public void carregarProcessosExistente(String conteudo) {
        if (conteudo != null && !conteudo.isEmpty()) {
            this.conteudoProcessosCSV = new StringBuilder(conteudo);
            this.cabecalhoProcessosEscrito = true;
        }
    }

    public boolean temProcessos() {
        return !processosList.isEmpty() ||
                (conteudoProcessosCSV != null && conteudoProcessosCSV.length() > 0);
    }

    public String getEmpresaNome() {
        return empresaNome;
    }

    public String getServidorNome() {
        return servidorNome;
    }

    public String getConteudoCSV() {
        return conteudoCSV.toString();
    }

    public String getConteudoProcessosCSV() {
        return conteudoProcessosCSV.toString();
    }
}

class Processo {
    private int id;
    private String servidor;
    private String timestamp;
    private String nome;
    private Double usoMemoria;

    public Processo(int id, String servidor, String timestamp, String nome, Double usoMemoria) {
        this.id = id;
        this.servidor = servidor;
        this.timestamp = timestamp;
        this.nome = nome;
        this.usoMemoria = usoMemoria;
    }

    public int getId() {
        return id;
    }

    public String getServidor() {
        return servidor;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public String getNome() {
        return nome;
    }

    public double getUsoMemoria() {
        return usoMemoria;
    }
}