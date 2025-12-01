package school.sptech;

import java.io.IOException;
import java.io.InputStream;

import com.fasterxml.jackson.databind.ObjectMapper;


public class JiraMapper {
    public JiraSearchResponse map(InputStream inputStream) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        return mapper.readValue(inputStream, JiraSearchResponse.class);
    }
}
