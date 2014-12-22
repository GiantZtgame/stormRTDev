package storm.qule_util.mgame;

/**
 * Created by wangxufeng on 2014/12/12.
 */
public class platform2ifabroad {
    public platform2ifabroad() {

    }

    /**
     * 根据平台号确定是否是国内1或者海外0
     * @param platform_id
     * @return
     */
    public static Integer turnPlatform2ifabroad(String platform_id) {
        //ifAbroad = 1 : 国内
        //ifAbroad = 0 : 海外
        Integer ifAbroad = 1;

        Integer platform_id_int = Integer.parseInt(platform_id);

        if (platform_id_int > 100) {
            ifAbroad = 0;
        }

        return ifAbroad;
    }
}
