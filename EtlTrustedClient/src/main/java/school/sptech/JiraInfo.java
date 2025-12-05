package school.sptech;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.joda.time.LocalDate;

import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;

public class JiraInfo {

    private static final String CLIENT_BUCKET = "nextrail-client-log";
    private static final S3Service s3 = new S3Service();


    public static void jiraMain() {
        // credencias do jira
        final String jiraEmail = Credenciais.getJiraEmail();
        final String jiraApiToken = Credenciais.getJiraApiTokenAdmin();
        final String jiraAuthToken = Base64.getEncoder().encodeToString(
                (jiraEmail + ":" + jiraApiToken).getBytes(StandardCharsets.UTF_8)
        );

        List<Suporte> listaSuporte = new ArrayList<>();

        try {
            // criando a conexao

            final String fields = "created,assignee,resolutiondate";
            final String expand = "changelog";

            final String jqlQuery = "project = \"AAC\" AND resolutiondate >= startOfDay(-1) AND resolutiondate < startOfDay()";
            String encodedJql = URLEncoder.encode(jqlQuery, StandardCharsets.UTF_8);

            URL url = new URL("https://nextrailsuporte.atlassian.net/rest/api/3/search/jql?jql="+encodedJql+"&fields="+fields+"&expand="+expand);
            System.out.println("URL: "+url.toString());

            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Basic " + jiraAuthToken);
            conn.setRequestProperty("Accept", "application/json");



            int responseCode = conn.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                // printa o json como string
                //System.out.println("Sucesso! responseCode: " + responseCode);
                //BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
                //String linha;
                //StringBuilder response = new StringBuilder();
                //while ((linha = in.readLine()) != null) {
                //    response.append(linha);
               // }
               // System.out.println("Resposta: " + response);

                System.out.println("MAPEANDO JSON...");
                JiraMapper mapper = new JiraMapper();
                JiraSearchResponse jiraSearchResponses = mapper.map(conn.getInputStream());
                System.out.println("QTD DE ISSUES: " + jiraSearchResponses.getIssues().size());

                jiraSearchResponses.printValores();

                System.out.println("========================");
                System.out.println("CRIANDO JSON CLIENT...");
                System.out.println("========================");

                DateTimeFormatter JIRA_DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSZ");

                for (JiraSearchResponse.Issue issue : jiraSearchResponses.getIssues()) {

                    JiraSearchResponse.ChangeLog changeLog = issue.getChangeLog();
                    JiraSearchResponse.FieldsJson fieldsJson = issue.getFields();
                    JiraSearchResponse.Assignee assignee = fieldsJson.getAssignee();

                    Integer id = issue.getId();

                    String displayName = assignee.getDisplayName();


                    //String resolutionString = fieldsJson.getResolutionDate();
                    //String createdIssueDate = fieldsJson.getCreated();

                    OffsetDateTime resolutionDate = OffsetDateTime.parse(fieldsJson.getResolutionDate(), JIRA_DATE_FORMATTER);
                    OffsetDateTime createdIssueDate = OffsetDateTime.parse(fieldsJson.getCreated(), JIRA_DATE_FORMATTER);

                    for (JiraSearchResponse.History history : changeLog.getHistories()) {

                        //String createdHistoryDate  = history.getCreated();

                        OffsetDateTime createdHistoryDate = OffsetDateTime.parse(history.getCreated(), JIRA_DATE_FORMATTER);

                        for (JiraSearchResponse.ChangeItem changeItem : history.getChangeItems()) {

                            String field = changeItem.getField();
                            String fromString = changeItem.getFromString();
                            String toString = changeItem.getToString();

                            if (field.equals("assignee" ) && fromString == null && toString != null) {

                                System.out.println("==========================================");
                                System.out.println("Primeira alteração do issue: " + id);
                                System.out.println("Assignee atual (TICKETS P SUP): " + displayName);
                                System.out.println("Criação do issue (MTTA): " +
                                        createdIssueDate.toString());
                                System.out.println("Data da primeira atribuição (MTTA): " +
                                        createdHistoryDate.toString());
                                System.out.println("");
                                System.out.println("Data de resolução: " + resolutionDate.toString());
                                System.out.println("Campo alterado: " + field);
                                System.out.println("Atribuido anterior (tem q ser null): " + fromString);
                                System.out.println("Primeiro atribuido (pode nao ser o atual): " +
                                        toString);

                                Suporte suporte = encontrarSuporte(listaSuporte, displayName);

                                if (suporte == null) {
                                    suporte = new Suporte(displayName);
                                    listaSuporte.add(suporte);
                                }

                                // calcula o tta em milisegundos, para ficar mais facil no front
                                Duration timeToAcknowledge = Duration.between(createdIssueDate, createdHistoryDate);
                                long timeToAcknowledgeMilis = timeToAcknowledge.toMillis();
                                System.out.println("DURACAO DO TTA MILISEGUNDOS: " + timeToAcknowledgeMilis);

                                suporte.qtdTickets++;
                                suporte.datasMtta.add(new DatasMtta(createdIssueDate.toString(), createdHistoryDate.toString(),timeToAcknowledgeMilis));

                            }
                        }
                    }
                }

                try {
                    ListObjectsV2Response empresas = s3.listarPastas(CLIENT_BUCKET);

                    for (CommonPrefix empresaPrefix : empresas.commonPrefixes()) {
                        String empresa = empresaPrefix.prefix().replace("/", "");

                        String jiraInfoPath = empresa + "/JiraInfo/";

                        ListObjectsV2Response jiraFolderCheck = s3.listarComPrefixo(CLIENT_BUCKET, jiraInfoPath, false);
                        boolean pastaExiste = !jiraFolderCheck.contents().isEmpty() ||
                                jiraFolderCheck.commonPrefixes().stream()
                                        .anyMatch(cp -> cp.prefix().equals(jiraInfoPath));

                        if (!pastaExiste) {
                            String dummyKey = jiraInfoPath + ".keep";
                            s3.enviarJsonObject(CLIENT_BUCKET, dummyKey, "{}");
                            System.out.println("Pasta JiraInfo criada para empresa: " + empresa);
                        }

                        String key = String.format("%s/JiraInfo/Jira-%s.json",
                                empresa,
                                LocalDate.now());
                        s3.enviarJsonObject(CLIENT_BUCKET, key, listaSuporte);
                        System.out.println("Salvo em: " + key);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }


            } else {
                // Tenta ler o corpo da resposta de ERRO
                InputStream errorStream = conn.getErrorStream();
                String errorBody = "";
                if (errorStream != null) {
                    try (BufferedReader errIn = new BufferedReader(new InputStreamReader(errorStream))) {
                        String errLine;
                        StringBuilder errorResponse = new StringBuilder();
                        while ((errLine = errIn.readLine()) != null) {
                            errorResponse.append(errLine);
                        }
                        errorBody = errorResponse.toString();
                    }
                }

                System.err.println("ERRO! responseCode: " + responseCode);
                System.err.println("Mensagem: " + conn.getResponseMessage());
                if (!errorBody.isEmpty()) {
                    System.err.println("Corpo da Resposta de Erro (Jira): " + errorBody);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class DatasMtta {
        public String dataCriacaoTicket;
        public String dataAtribuicaoTicket;
        public long timeToAcknowledgeMilis;

        public DatasMtta(String dataCriacaoTicket, String dataAtribuicaoTicket, long timeToAcknowledgeMilis) {
            this.dataCriacaoTicket = dataCriacaoTicket;
            this.dataAtribuicaoTicket = dataAtribuicaoTicket;
            this.timeToAcknowledgeMilis = timeToAcknowledgeMilis;
        }
    }

    private static class Suporte {
        public String nome; // displayName
        public Integer qtdTickets = 0; // vai somando os tickets no for
        public List<DatasMtta> datasMtta = new ArrayList<>();

        public Suporte(String nome) {
            this.nome = nome;
        }
    }

    private static Suporte encontrarSuporte(List<Suporte> lista, String nomeBuscado) {
        for (Suporte suporte : lista) {
            if (suporte.nome.equals(nomeBuscado)) {
                return suporte;
            }
        }
        return null;
    }


}
