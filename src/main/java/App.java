import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Scanner;

class RegistroDeLog {
    private LocalDateTime dataHora;
    private String usuario;
    private String maquina;
    private String operacao;
    private String criticidade;

    public RegistroDeLog(LocalDateTime dataHora, String usuario, String maquina, String operacao, String criticidade) {
        this.dataHora = dataHora;
        this.usuario = usuario;
        this.maquina = maquina;
        this.operacao = operacao;
        this.criticidade = criticidade;
    }

    // Getter (acessador) para pegar a criticidade de fora da classe
    public String getCriticidade() {
        return criticidade;
    }

    // Getter para pegar o nome do usuário de fora da classe
    public String getUsuario() {
        return usuario;
    }

    // Getter para pegar o nome do usuário de fora da classe
    public String getMaquina() {
        return maquina;
    }


    // toString sobrescrito: transforma o objeto em um texto legível
    @Override
    public String toString() {
        return String.format("[%s] Usuário: %s | Máquina: %s | Operação: %s | Criticidade: %s",
                dataHora, usuario, maquina, operacao, criticidade);
    }
}

public class App {                                    // Classe principal que contém o método main
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);     // Cria o objeto Scanner para ler teclado
        List<RegistroDeLog> logs = new ArrayList<>(); // Cria uma lista dinâmica para armazenar os logs

        // Adicionando registros prontos
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Felipe", "Servidor01", "Backup executado", "PEQUENA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Celia", "Servidor02", "Reinicialização", "ALTA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Bianca", "Servidor03", "Atualização de sistema", "MÉDIA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Igor", "Servidor04", "Instalação de software crítico", "ALTA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Aoki", "Servidor01", "Verificação de logs", "PEQUENA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Hideo", "Servidor05", "Troca de hardware", "MÉDIA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Kheyla", "Servidor03", "Aplicação de patch de segurança", "ALTA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Brandão", "Servidor02", "Limpeza de arquivos temporários", "PEQUENA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Clara", "Servidor04", "Configuração de rede", "MÉDIA"));
        logs.add(new RegistroDeLog(LocalDateTime.now(), "Pedro", "Servidor05", "Teste de redundância", "PEQUENA"));

        System.out.println("=== Sistema de Registro de Logs ===");

        boolean continuar = true;   // Controle do loop para adicionar vários logs
        while (continuar) {         // Enquanto o usuário quiser, continua pedindo informações
            System.out.print("\nDigite o nome do usuário: ");
            String usuario = scanner.nextLine();

            System.out.print("Digite o nome da máquina: ");
            String maquina = scanner.nextLine();

            System.out.print("Digite a operação realizada: ");
            String operacao = scanner.nextLine();

            System.out.print("Digite a criticidade (ALTA / MÉDIA / PEQUENA): ");
            String criticidade = scanner.nextLine().toUpperCase();

            logs.add(new RegistroDeLog(LocalDateTime.now(), usuario, maquina, operacao, criticidade));

            System.out.print("\nDeseja adicionar outro log? (s/n): ");
            String resposta = scanner.nextLine();
            if (resposta.equalsIgnoreCase("n")) {
                continuar = false;
            }
        }

        // Exibe todos os logs cadastrados
        System.out.println("\n=== Lista de Logs ===");
        for (RegistroDeLog log : logs) {  // Para cada log na lista, imprime na tela
            System.out.println(log.toString());      // Aqui chama o método toString()
        }

        // Ordenando por usuário
        Collections.sort(logs, Comparator.comparing(RegistroDeLog::getUsuario));

        System.out.println("\n=== Lista de Logs Ordenada por Usuário ===");
        for (RegistroDeLog log : logs) {
            System.out.println(log.toString());
        }

        // Ordenando por criticidade
        Collections.sort(logs, Comparator.comparing(RegistroDeLog::getCriticidade));

        System.out.println("\n=== Lista de Logs Ordenada por Criticidade ===");
        for (RegistroDeLog log : logs) {
            System.out.println(log.toString());
        }

        // Ordenando por maquina
        Collections.sort(logs, Comparator.comparing(RegistroDeLog::getMaquina));

        System.out.println("\n=== Lista de Logs Ordenada por Máquina ===");
        for (RegistroDeLog log : logs) {
            System.out.println(log.toString());
        }

        scanner.close();
    }
}