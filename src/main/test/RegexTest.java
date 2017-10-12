import java.util.regex.Pattern;

/**
 * Created by rembau on 2017/3/19.
 */
public class RegexTest {
    public static void main(String args[]) {
        Pattern.compile("\\|start\\|data_type:(.*),topic:(.*),toEs:(.*),index:(.*),type:(.*)\\|end\\|");
    }
}
