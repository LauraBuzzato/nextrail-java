package school.sptech;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Notificador {

    private final String slackWebhookUrl = Credenciais.getSlackWebHookUrl();
    private final String jiraUrl = Credenciais.getJiraUrl();


    private final String jiraEmail = Credenciais.getJiraEmail();
    private final String jiraApiToken = Credenciais.getJiraApiToken();
    private final String jiraAuthToken = Base64.getEncoder().encodeToString(
            (jiraEmail + ":" + jiraApiToken).getBytes(StandardCharsets.UTF_8)
    );

    private final String jiraProjeto = "AAC";

    public void enviarSlack(String mensagem) {
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

        } catch (Exception e) {
            System.out.println("Erro ao enviar para Slack: " + e.getMessage());
        }
    }

    public void criarJira(String titulo, String descricao) {

        String[] tiposIssue = {"Bug"};

        for (String tipo : tiposIssue) {
            if (tentarCriarJiraComTipo(titulo, descricao, tipo)) {
                break;
            }
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

            System.out.println("Tentando criar Jira com tipo: " + issueType);

            try (OutputStream os = con.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }

            int responseCode = con.getResponseCode();

            if (responseCode == 200 || responseCode == 201) {
                System.out.println("Ticket Jira criado com sucesso! Tipo: " + issueType);
                return true;
            } else {
                System.out.println("Falhou com tipo: " + issueType + " (Response: " + responseCode + ")");
                return false;
            }

        } catch (Exception e) {
            System.out.println("Erro com tipo " + issueType + ": " + e.getMessage());
            return false;
        }
    }

    public void enviarRelatorioConsolidado(String titulo, String mensagem) {
        System.out.println("ENVIANDO RELATÓRIO CONSOLIDADO:");

        System.out.println("\nEnviando para Slack...");
        enviarSlack(mensagem);

        System.out.println("\nEnviando para Jira...");
        criarJira(titulo, mensagem);
    }
}
