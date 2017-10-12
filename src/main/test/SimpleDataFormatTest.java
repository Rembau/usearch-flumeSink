import java.text.MessageFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by rembau on 2017/3/27.
 */
public class SimpleDataFormatTest {
    public static void main(String args[]) {
        String format = "'index-'yyyy-MM-dd";
        String s = "index-{yyyy-MM-dd}";
        String index = null;

        if (s.matches(".+\\{.+}")) {

            MessageFormat messageFormat = new MessageFormat("{0}'{'{1}'}'");
            try {
                Object[] objects = messageFormat.parse(s);
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat(objects[1].toString());
                index = objects[0] + simpleDateFormat.format(new Date());
            } catch (ParseException e) {
                e.printStackTrace();
            }
        } else {
            index = s;
        }

        System.out.println(index);
    }
}
