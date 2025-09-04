import java.time.LocalDateTime;

class RegistroDeLog {
    private LocalDateTime dataHora;
    private String usuario;
    private String maquina;
    private String evento;
    private String criticidade;

    public RegistroDeLog(LocalDateTime dataHora, String usuario, String maquina, String operacao, String criticidade) {
        this.dataHora = dataHora;
        this.usuario = usuario;
        this.maquina = maquina;
        this.evento = operacao;
        this.criticidade = criticidade;
    }

    public String getCriticidade() {
        return criticidade;
    }

    public String getUsuario() {
        return usuario;
    }

    public String getMaquina() {
        return maquina;
    }

    @Override
    public String toString() {
        return String.format("[%s] Usuário: %s | Máquina: %s | Evento: %s | Criticidade: %s",
                dataHora, usuario, maquina, evento, criticidade);
    }
}