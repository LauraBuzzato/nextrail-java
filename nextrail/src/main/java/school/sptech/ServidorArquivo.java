package school.sptech;

public class ServidorArquivo {
    private String empresaNome;
    private String servidorNome;
    private StringBuilder conteudoCSV;
    private boolean cabecalhoEscrito = false;

    public ServidorArquivo(String empresaNome, String servidorNome) {
        this.empresaNome = empresaNome;
        this.servidorNome = servidorNome;
        this.conteudoCSV = new StringBuilder();
    }

    public void adicionarLinha(String cabecalho, String linha) {
        if (!cabecalhoEscrito) {
            conteudoCSV.append(cabecalho).append("\n");
            cabecalhoEscrito = true;
        }
        conteudoCSV.append(linha).append("\n");
    }

    public String getEmpresaNome() { return empresaNome; }
    public String getServidorNome() { return servidorNome; }
    public String getConteudoCSV() { return conteudoCSV.toString(); }
}