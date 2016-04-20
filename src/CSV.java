import java.util.ArrayList;
import java.util.List;

/**
 * Created by marvin on 4/19/16.
 */
public class CSV {

    public static List<String> split(String line, String delim) {

        List<String> columns = new ArrayList<>();
        if (!line.isEmpty()) {
            int delimIndex;
            while ((delimIndex = line.indexOf(delim)) != -1) {
                columns.add(line.substring(0, delimIndex));
                if (delimIndex + delim.length() >= line.length()) {
                    // trailing comma for some reason
                    return columns;
                }
                line = line.substring(delimIndex + delim.length());
            }
            columns.add(line);
        }
        return columns;
    }

    public static String join(List<String> strs, String delim) {

        StringBuilder sb = new StringBuilder();
        if (!strs.isEmpty()) {
            for (String str : strs) {
                if (str != null) {
                    sb.append(str);
                }
                sb.append(delim);
            }
            for (int i = 0; i < delim.length(); i++) {
                sb.deleteCharAt(sb.length() - 1 - i);
            }
        }
        return sb.toString();
    }
}
