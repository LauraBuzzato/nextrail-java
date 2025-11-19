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
    private int consecutivasCpu = 0;
    private int consecutivasRam = 0;
    private int consecutivasDisco = 0;
    private int ultimaGravidadeCpu = -1;
    private int ultimaGravidadeRam = -1;
    private int ultimaGravidadeDisco = -1;

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

    public void adicionarLeitura(int gravidadeCpu, int gravidadeRam, int gravidadeDisco) {
        historicoCpu[posicaoHistorico] = gravidadeCpu;
        historicoRam[posicaoHistorico] = gravidadeRam;
        historicoDisco[posicaoHistorico] = gravidadeDisco;
        posicaoHistorico = (posicaoHistorico + 1) % leiturasParaAlerta;

        if (gravidadeCpu == ultimaGravidadeCpu && gravidadeCpu > 0) {
            consecutivasCpu++;
        } else {
            consecutivasCpu = 1;
            ultimaGravidadeCpu = gravidadeCpu;
        }

        if (gravidadeRam == ultimaGravidadeRam && gravidadeRam > 0) {
            consecutivasRam++;
        } else {
            consecutivasRam = 1;
            ultimaGravidadeRam = gravidadeRam;
        }

        if (gravidadeDisco == ultimaGravidadeDisco && gravidadeDisco > 0) {
            consecutivasDisco++;
        } else {
            consecutivasDisco = 1;
            ultimaGravidadeDisco = gravidadeDisco;
        }
    }

    public boolean deveAlertarCpu(int gravidadeAtual) {
        return consecutivasCpu >= leiturasParaAlerta &&
                gravidadeAtual > 0 &&
                gravidadeAtual == ultimaGravidadeCpu;
    }

    public boolean deveAlertarRam(int gravidadeAtual) {
        return consecutivasRam >= leiturasParaAlerta &&
                gravidadeAtual > 0 &&
                gravidadeAtual == ultimaGravidadeRam;
    }

    public boolean deveAlertarDisco(int gravidadeAtual) {
        return consecutivasDisco >= leiturasParaAlerta &&
                gravidadeAtual > 0 &&
                gravidadeAtual == ultimaGravidadeDisco;
    }

    public void resetarContadoresCpu() {
        consecutivasCpu = 0;
        ultimaGravidadeCpu = -1;
    }

    public void resetarContadoresRam() {
        consecutivasRam = 0;
        ultimaGravidadeRam = -1;
    }

    public void resetarContadoresDisco() {
        consecutivasDisco = 0;
        ultimaGravidadeDisco = -1;
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
}