package school.sptech;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.util.*;

public class CsvConverter {

    public static List<Map<String, String>> csvToList(String csv) throws Exception {

        CsvMapper mapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema()
                .withHeader()
                .withColumnSeparator(';');

        MappingIterator<Map<String, String>> it =
                mapper.readerFor(new TypeReference<Map<String, String>>() {})
                        .with(schema)
                        .readValues(csv);

        List<Map<String, String>> lista = new ArrayList<>();

        while (it.hasNext()) {
            lista.add(it.next());
        }

        return lista;
    }
}
