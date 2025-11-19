package school.sptech;

public class ComponenteLimites {
    private double cpuLimiteBaixo;
    private double cpuLimiteMedio;
    private double cpuLimiteAlto;
    private double ramLimiteBaixo;
    private double ramLimiteMedio;
    private double ramLimiteAlto;
    private double discoLimiteBaixo;
    private double discoLimiteMedio;
    private double discoLimiteAlto;

    public void setCpuLimiteBaixo(double limite) {
        this.cpuLimiteBaixo = limite;
    }

    public void setCpuLimiteMedio(double limite) {
        this.cpuLimiteMedio = limite;
    }

    public void setCpuLimiteAlto(double limite) {
        this.cpuLimiteAlto = limite;
    }

    public void setRamLimiteBaixo(double limite) {
        this.ramLimiteBaixo = limite;
    }

    public void setRamLimiteMedio(double limite) {
        this.ramLimiteMedio = limite;
    }

    public void setRamLimiteAlto(double limite) {
        this.ramLimiteAlto = limite;
    }

    public void setDiscoLimiteBaixo(double limite) {
        this.discoLimiteBaixo = limite;
    }

    public void setDiscoLimiteMedio(double limite) {
        this.discoLimiteMedio = limite;
    }

    public void setDiscoLimiteAlto(double limite) {
        this.discoLimiteAlto = limite;
    }

    public double getCpuLimiteBaixo() {
        return cpuLimiteBaixo;
    }

    public double getCpuLimiteMedio() {
        return cpuLimiteMedio;
    }

    public double getCpuLimiteAlto() {
        return cpuLimiteAlto;
    }

    public double getRamLimiteBaixo() {
        return ramLimiteBaixo;
    }

    public double getRamLimiteMedio() {
        return ramLimiteMedio;
    }

    public double getRamLimiteAlto() {
        return ramLimiteAlto;
    }

    public double getDiscoLimiteBaixo() {
        return discoLimiteBaixo;
    }

    public double getDiscoLimiteMedio() {
        return discoLimiteMedio;
    }

    public double getDiscoLimiteAlto() {
        return discoLimiteAlto;
    }

    public int verificarGravidadeCpu(double valor) {
        return verificarGravidade(valor, cpuLimiteBaixo, cpuLimiteMedio, cpuLimiteAlto);
    }

    public int verificarGravidadeRam(double valor) {
        return verificarGravidade(valor, ramLimiteBaixo, ramLimiteMedio, ramLimiteAlto);
    }

    public int verificarGravidadeDisco(double valor) {
        return verificarGravidade(valor, discoLimiteBaixo, discoLimiteMedio, discoLimiteAlto);
    }

    private int verificarGravidade(double valor, double baixo, double medio, double alto) {
        if (valor >= alto) return 3;
        if (valor >= medio) return 2;
        if (valor >= baixo) return 1;
        return 0;
    }
}