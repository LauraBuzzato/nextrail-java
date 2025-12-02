package school.sptech;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

public class Notificador {

    private final String slackWebhookUrl = Credenciais.getSlackWebHookUrl();
    private final String jiraUrl = Credenciais.getJiraUrl();
    private final String jiraEmail = Credenciais.getJiraEmail();
    private final String jiraApiToken = Credenciais.getJiraApiToken();
    private final String jiraAuthToken = Base64.getEncoder().encodeToString(
            (jiraEmail + ":" + jiraApiToken).getBytes(StandardCharsets.UTF_8)
    );
    private final String jiraProjeto = "AAC";

    private static LocalDateTime ultimoEnvio = null;

    private boolean podeEnviarNotificacoes() {
        if (ultimoEnvio == null) {
            return true;
        }

        long horasPassadas = ChronoUnit.HOURS.between(ultimoEnvio, LocalDateTime.now());
        return horasPassadas >= 1;
    }

    private void atualizarUltimoEnvio() {
        ultimoEnvio = LocalDateTime.now();
        System.out.println("Último envio atualizado: " + ultimoEnvio);
    }

    public void enviarSlack(String mensagem) {
        if (!podeEnviarNotificacoes()) {
            System.out.println("Slack ignorado - ainda não passou 1 hora");
            return;
        }

        try {
            URL url = new URL(slackWebhookUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setDoOutput(true);

            String json = String.format("{\"text\": \"%s\"}",
                    mensagem.replace("\"", "\\\"")
                            .replace("\n", "\\n")
                            .replace("\r", ""));

            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = con.getResponseCode();
            System.out.println("Slack Response: " + responseCode);
            atualizarUltimoEnvio();

        } catch (Exception e) {
            System.out.println("Erro Slack: " + e.getMessage());
        }
    }

    private boolean tentarCriarJiraComTipo(String titulo, String descricao, String issueType) {
        try {
            URL url = new URL(jiraUrl);
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");
            con.setRequestProperty("Authorization", "Basic " + jiraAuthToken);
            con.setRequestProperty("Accept", "application/json");
            con.setDoOutput(true);

            String json = """
                {
                  "fields": {
                    "project": {"key": "%s"},
                    "summary": "%s",
                    "description": {
                      "type": "doc",
                      "version": 1,
                      "content": [{
                        "type": "paragraph",
                        "content": [{"type": "text", "text": "%s"}]
                      }]
                    },
                    "issuetype": {"name": "%s"}
                  }
                }
                """.formatted(
                    jiraProjeto,
                    titulo.replace("\"", "\\\""),
                    descricao.replace("\"", "\\\"").replace("\n", "\\\\n"),
                    issueType
            );

            System.out.println("Tentando criar Jira: " + issueType);

            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = con.getResponseCode();

            if (responseCode == 200 || responseCode == 201) {
                System.out.println("Jira criado: " + issueType);
                return true;
            } else {
                System.out.println("Falhou: " + issueType + " (Response: " + responseCode + ")");
                return false;
            }

        } catch (Exception e) {
            System.out.println("Erro Jira " + issueType + ": " + e.getMessage());
            return false;
        }
    }

    public void criarJiraTicketIndividual(String titulo, String mensagem) {
        if (!podeEnviarNotificacoes()) {
            System.out.println("Jira ignorado - ainda não passou 1 hora");
            return;
        }

        String[] tiposIssue = {"Task", "Bug"};

        for (String tipo : tiposIssue) {
            if (tentarCriarJiraComTipo(titulo, mensagem, tipo)) {
                System.out.println("Ticket individual criado: " + titulo);
                atualizarUltimoEnvio();
                break;
            }
        }
    }

    public void enviarRelatorioConsolidado(String titulo, String mensagem) {
        if (!podeEnviarNotificacoes()) {
            System.out.println("Relatório consolidado ignorado - ainda não passou 1 hora");
            return;
        }

        System.out.println("Enviando relatório consolidado...");
        enviarSlack(mensagem);
        criarJiraTicketIndividual(titulo, mensagem);
    }
}