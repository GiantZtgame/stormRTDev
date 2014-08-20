package storm.qule_util;

import scala.util.parsing.combinator.testing.Str;

import java.security.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Pattern;

/**
 * Created by wangxufeng on 2014/7/16.
 */
public class date {
    private static String DATE_FORMAT_A_P = "yyyy-MM-dd HH:mm:ss";
    private static String DATE_FORMAT_A_N = "yyyyMMdd HH:mm:ss";

    private static String DATE_FORMAT_S_P = "yyyy-MM-dd";
    private static String DATE_FORMAT_S_N = "yyyyMMdd";

    public date() {
    }

    private static String getFormat(String datetime) {
        String df = DATE_FORMAT_A_P;
        Pattern p = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");
        Pattern p2 = Pattern.compile("[0-9]{4}[0-9]{2}[0-9]{2}");
        String[] d = datetime.split(" ");
        switch (d.length) {
            case 1:
                if (p.matcher(d[0]).matches()) df = DATE_FORMAT_S_P;
                if (p2.matcher(d[0]).matches()) df = DATE_FORMAT_S_N;
                break;
            case 2:
                if (p.matcher(d[0]).matches()) df = DATE_FORMAT_A_P;
                if (p2.matcher(d[0]).matches()) df = DATE_FORMAT_A_N;
                break;
        }
        return df;
    }

    public static long str2timestamp(String datetime_str) {
        return str2timestamp(datetime_str, getFormat(datetime_str));
    }

    public static long str2timestamp(String datetime_str, String fromFormat) {
        SimpleDateFormat format = new SimpleDateFormat(fromFormat);
        Date date = null;
        try {
            date = format.parse(datetime_str);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date.getTime()/1000;
    }

    public static String timestamp2str(Long timestamp, String toFormat) {
        String dateStr = "";
        Date date = new Date(timestamp*1000);
        SimpleDateFormat sdf = new SimpleDateFormat(toFormat);
        try {
            dateStr = sdf.format(date);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dateStr;
    }

}
