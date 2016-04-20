import java.io.IOException;
import java.util.*;

/**
 * Created by marvin on 4/7/16.
 */
public class Main {

    public static void main(String[] args) throws IOException {

//        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
        Metadata metadata = new Metadata("/home/marvin/Downloads/");
        Queue<Table> updateQueue = new LinkedList<>();
        QueryProcessor qp = new QueryProcessor(updateQueue);
        ColumnUpdater updater = new ColumnUpdater(updateQueue);
        updater.start();

        List<String> columns = new ArrayList<>();
        columns.add("first_name");
        columns.add("last_name");
        metadata.createTable("people", columns);
        Table table = metadata.get("people");
        Map<String, String> data = new HashMap<>();
        data.put("first_name", "Marvin");
        data.put("last_name", "Qian");
        qp.write(new Record(table, data));
    }
}
