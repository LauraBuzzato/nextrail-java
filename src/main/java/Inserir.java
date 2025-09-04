import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.time.LocalDateTime;

public class Inserir {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        List<RegistroDeLog> lista = new ArrayList<>();

        // Adicionando registros prontos
        lista.add(new RegistroDeLog(LocalDateTime.now(), "Felipe", "Servidor01", "Uso de CPU alto", "ALTA"));
        lista.add(new RegistroDeLog(LocalDateTime.now(), "Celia", "Servidor02", "Disco cheio", "MÉDIA"));
        lista.add(new RegistroDeLog(LocalDateTime.now(), "André", "Servidor03", "Temperatura elevada", "PEQUENA"));


        System.out.println("=== Sistema de Registro de Logs ===");

        boolean continuar = true;
        while (continuar) {
            System.out.print("\nDigite o nome do usuário: ");
            String usuario = scanner.nextLine();

            System.out.print("Digite o nome da máquina: ");
            String maquina = scanner.nextLine();

            System.out.print("Digite o evento realizado: ");
            String evento = scanner.nextLine();

            System.out.print("Digite a criticidade (ALTA / MÉDIA / PEQUENA): ");
            String criticidade = scanner.nextLine().toUpperCase();

            lista.add(new RegistroDeLog(LocalDateTime.now(), usuario, maquina, evento, criticidade));

            System.out.print("\nDeseja adicionar outro log? (s/n): ");
            String resposta = scanner.nextLine();
            if (resposta.equalsIgnoreCase("n")) {
                continuar = false;
            }
        }

        selectionSortPorCriticidade(lista);

        System.out.println("\n=== Lista de Logs Ordenados por Criticidade ===");
        for (RegistroDeLog log : lista) {
            System.out.println(log.toString());
        }

        scanner.close();
    }

    public static void selectionSortPorCriticidade(List<RegistroDeLog> lista) {
        int n = lista.size();
        for (int i = 0; i < n - 1; i++) {
            int menor = i;
            for (int j = i + 1; j < n; j++) {
                String criticidade1 = lista.get(j).getCriticidade();
                String criticidade2 = lista.get(menor).getCriticidade();

                if (criticidade1.compareToIgnoreCase(criticidade2) < 0) {
                    menor = j;
                }
            }
            RegistroDeLog temp = lista.get(i);
            lista.set(i, lista.get(menor));
            lista.set(menor, temp);
        }
    }
}
