import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by marvin on 4/7/16.
 */
public class Main {

    public static void main(String[] args) throws IOException {

        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
//        Metadata metadata = new Metadata("/home/marvin/Downloads/");
        List<String> columns = new ArrayList<>();
        columns.add("first_name");
        columns.add("last_name");
        metadata.createTable("people", columns);
        Table table = metadata.get("people");
        Map<String, String> data = new HashMap<>();
        data.put("first_name", "Marvin");
        data.put("last_name", "Qian");
        QueryProcessor.write(new Record(table, data));
        data.put("first_name", "Frank");
        data.put("last_name", "Wu");
        QueryProcessor.write(new Record(table, data));
        data.put("first_name", "Justin");
        data.put("last_name", "Zhang");
        QueryProcessor.write(new Record(table, data));
        List<Record> records = QueryProcessor.scan(table, columns);
        for (Record record : records) {
            System.out.println(record.getId() + ": " + record.getValues());
        }
    }
}
