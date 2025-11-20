package school.sptech;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CsvConverter {

    public static String csvToJson(String csv) throws Exception {
        CsvMapper csvMapper = new CsvMapper();
        CsvSchema csvSchema = CsvSchema.emptySchema()
                .withHeader()
                .withColumnSeparator(';');

        MappingIterator<Map<String, String>> it =
                csvMapper.readerFor(new TypeReference<Map<String, String>>() {})
                        .with(csvSchema)
                        .readValues(csv);

        List<Map<String, String>> rows = new ArrayList<>();

        while (it.hasNext()) {
            rows.add(it.next());
        }

        ObjectMapper jsonMapper = new ObjectMapper();
        return jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(rows);
    }
}
