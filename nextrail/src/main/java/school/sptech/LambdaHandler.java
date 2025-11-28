
package school.sptech;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.util.Map;

public class LambdaHandler implements RequestHandler<Map<String, String>, String> {

    @Override
    public String handleRequest(Map<String, String> event, Context context) {
        LambdaLogger logger = context.getLogger();

        try {
            logger.log("Iniciando processamento ETL via Lambda");

            String[] args = new String[0];

            Tratamento.main(args);

            logger.log("Processamento ETL concluído com sucesso");
            return "SUCCESS";

        } catch (Exception e) {
            logger.log("Erro no processamento: " + e.getMessage());
            e.printStackTrace();
            return "ERROR: " + e.getMessage();
        }
    }
}