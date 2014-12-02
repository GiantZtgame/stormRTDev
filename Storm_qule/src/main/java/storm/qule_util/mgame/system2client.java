package storm.qule_util.mgame;

/**
 * Created by wangxufeng on 2014/11/25.
 */
public class system2client {
    private final static String TOTAL   = "total";
    private final static String IOSJB   = "iosjb";
    private final static String IOS     = "ios";
    private final static String ANDROID = "android";

    public system2client() {

    }

    /**
     * 根据系统名称获取客户端类型ID
     * @param system
     * @return int
     */
    public static Integer turnSystem2ClientId(String system) {
        Integer client = 0;

        if (IOSJB.equals(system) || IOS.equals(system)) {
            client = 1;
        } else if (ANDROID.equals(system)) {
            client = 2;
        } else if (TOTAL.equals(system)) {
            client = 0;
        }

        return client;
    }

}
