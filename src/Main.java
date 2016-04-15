import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by marvin on 4/7/16.
 */
public class Main {

    public static void main(String[] args) throws IOException {

//        Metadata metadata = new Metadata("/Users/frankjwu/Downloads/");
        Metadata metadata = new Metadata("/home/marvin/Downloads/");
//        List<String> columns = new ArrayList<>();
//        columns.add("first_name");
//        columns.add("last_name");
//        metadata.createTable("people", columns);
        Table table = metadata.get("people");
        System.out.println(table.isPrimary("first_name"));
    }
}
