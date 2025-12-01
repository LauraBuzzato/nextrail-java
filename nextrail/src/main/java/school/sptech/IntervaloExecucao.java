package school.sptech;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class IntervaloExecucao {

    private static final int INTERVALO_MINUTOS = 60; // 1 hora

    public static boolean deveExecutar(LocalDateTime ultimaExecucao) {
        if (ultimaExecucao == null) {
            return true;
        }

        long minutosPassados = ChronoUnit.MINUTES.between(ultimaExecucao, LocalDateTime.now());
        return minutosPassados >= INTERVALO_MINUTOS;
    }

    public static LocalDateTime getHorarioAtual() {
        return LocalDateTime.now();
    }
}