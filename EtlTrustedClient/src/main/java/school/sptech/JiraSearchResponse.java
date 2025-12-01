package school.sptech;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

// estrutura das classes:
//
// JiraSearchResponse List<Issue> -> 
// Issue -> 
// ChangeLog List<History = histories> ->    &&  FieldsJson (created) ->
// History List<ChangeItem> (created) ->     //    Assignee (displayName)
// ChangeItem (field, toString, fromString)  //
//
// o fromString serve para saber se ele so foi reatribuido, o que nao contaria para o MTTA

@JsonIgnoreProperties(ignoreUnknown = true)
public class JiraSearchResponse {

    @JsonProperty("issues")
    private List<Issue> issues;

    public List<Issue> getIssues() { return issues; }
    public void setIssues(List<Issue> issues) { this.issues = issues; }

    public void printValores() {
        System.out.println("========================");
        System.out.println("JiraSearchResponse:");

        for (Issue issue : issues) {

            System.out.println("========================");
            System.out.println("Issue id: " + issue.getId());
            ChangeLog changeLog = issue.getChangeLog();
            System.out.println("Histories:");

            int i = 0;
            for (History history : changeLog.getHistories()) {
                
                System.out.println("history " + i + ":");
                System.out.println("    created: " + history.created);
                for (ChangeItem changeItem : history.getChangeItems()) {

                    System.out.println("    field: " + changeItem.field);
                    System.out.println("    fromString: " + changeItem.fromString);
                    System.out.println("    toString: " + changeItem.toString);
                }
                i++;
            }
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Issue {

        @JsonProperty("id")
        private Integer id;

        @JsonProperty("changelog")
        private ChangeLog changeLog; 

        @JsonProperty("fields")
        private FieldsJson fields;

        public Integer getId() { return id; }
        public void setId(Integer id) { this.id = id; }

        public ChangeLog getChangeLog() { return changeLog; }
        public void setChangeLog(ChangeLog changeLog) { this.changeLog = changeLog; }

        public FieldsJson getFields() { return fields; }
        public void setFields(FieldsJson fields) { this.fields = fields; }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ChangeLog {
        
        @JsonProperty("histories")
        private List<History> histories;

        public List<History> getHistories() { return histories; }
        public void setHistories(List<History> histories) { this.histories = histories; }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldsJson {
    
        @JsonProperty("created")
        private String created;

        @JsonProperty("assignee")
        private Assignee assignee;

        @JsonProperty("resolutiondate")
        private String resolutionDate;

        public String getCreated() { return created; }
        public void setCreated(String created) { this.created = created; }

        public Assignee getAssignee() { return assignee; }
        public void setAssignee(Assignee assignee) { this.assignee = assignee; }

        public String getResolutionDate() { return resolutionDate; }
        public void getResolutionDate(String resolutionDate) { this.resolutionDate = resolutionDate; }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Assignee {

        @JsonProperty("displayName")
        private String displayName;

        public String getDisplayName() { return displayName; }
        public void setDisplayName(String displayName) { this.displayName = displayName; }
    }

    
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class History {

        @JsonProperty("created")
        private String created;

        @JsonProperty("items")
        private List<ChangeItem> items;

        public String getCreated() { return created; }
        public void setCreated(String created) { this.created = created; }

        public List<ChangeItem> getChangeItems() { return items; }
        public void setItems(List<ChangeItem> items) { this.items = items; }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ChangeItem {

        @JsonProperty("field")
        private String field;

        @JsonProperty("toString")
        private String toString; 

        @JsonProperty("fromString")
        private String fromString;

        public String getField() { return field; }
        public void setField(String field) { this.field = field; }

        public String getToString() { return toString; }
        public void setToString(String toString) { this.toString = toString; }
        
        public String getFromString() { return fromString; }
        public void setFromString(String fromString) { this.fromString = fromString; }
    }
}
