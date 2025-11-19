package school.sptech;

public class ServidorConfig {
    private int id;
    private String nome;
    private String empresaNome;
    private int empresaId;
    private int leiturasParaAlerta;
    private int[] historicoCpu;
    private int[] historicoRam;
    private int[] historicoDisco;
    private int posicaoHistorico = 0;
    private int leiturasConsecutivasAtuais = 0;
    private int ultimaGravidade = -1;


    public ServidorConfig(int id, String nome, String empresaNome, int empresaId, int leiturasParaAlerta) {
        this.id = id;
        this.nome = nome;
        this.empresaNome = empresaNome;
        this.empresaId = empresaId;
        this.leiturasParaAlerta = leiturasParaAlerta;
        this.historicoCpu = new int[leiturasParaAlerta];
        this.historicoRam = new int[leiturasParaAlerta];
        this.historicoDisco = new int[leiturasParaAlerta];
    }

    public boolean verificarLeiturasConsecutivas(int gravidadeAtual, int leiturasNecessarias) {
        if (gravidadeAtual == ultimaGravidade && gravidadeAtual > 0) {
            leiturasConsecutivasAtuais++;
        } else {
            leiturasConsecutivasAtuais = 1;
            ultimaGravidade = gravidadeAtual;
        }

        return leiturasConsecutivasAtuais >= leiturasNecessarias && gravidadeAtual > 0;
    }

    public void resetarLeiturasConsecutivas() {
        leiturasConsecutivasAtuais = 0;
        ultimaGravidade = -1;
    }


    public int getId() {
        return id;
    }

    public String getNome() {
        return nome;
    }

    public String getEmpresaNome() {
        return empresaNome;
    }

    public int getEmpresaId() {
        return empresaId;
    }

    public int getLeiturasParaAlerta() {
        return leiturasParaAlerta;
    }

    public void adicionarLeitura(int gravidadeCpu, int gravidadeRam, int gravidadeDisco) {
        historicoCpu[posicaoHistorico] = gravidadeCpu;
        historicoRam[posicaoHistorico] = gravidadeRam;
        historicoDisco[posicaoHistorico] = gravidadeDisco;
        posicaoHistorico = (posicaoHistorico + 1) % leiturasParaAlerta;
    }

    public boolean deveAlertarCpu(int gravidadeAtual) {
        return verificarHistorico(historicoCpu, gravidadeAtual);
    }

    public boolean deveAlertarRam(int gravidadeAtual) {
        return verificarHistorico(historicoRam, gravidadeAtual);
    }

    public boolean deveAlertarDisco(int gravidadeAtual) {
        return verificarHistorico(historicoDisco, gravidadeAtual);
    }

    private boolean verificarHistorico(int[] historico, int gravidadeAtual) {
        for (int i = 0; i < leiturasParaAlerta; i++) {
            if (historico[i] != gravidadeAtual) {
                return false;
            }
        }
        return gravidadeAtual > 0;
    }
}