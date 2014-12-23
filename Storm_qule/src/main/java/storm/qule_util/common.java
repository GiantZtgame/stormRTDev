package storm.qule_util;

/**
 * Created by wangxufeng on 2014/12/23.
 */
public class common {
    public void common() {
    }

    public static boolean isNumber(String numStr) {
        try {
            Double d = Double.parseDouble(numStr);
        } catch (NumberFormatException e) {
            return false;
        }
        return true;
    }
}
