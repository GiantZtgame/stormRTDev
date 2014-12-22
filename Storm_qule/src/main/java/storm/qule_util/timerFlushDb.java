package storm.qule_util;

import java.io.Serializable;

/**
 * Created by wangxufeng on 2014/12/4.
 */
public class timerFlushDb extends asyncTimer implements Serializable {
    private Integer _timeout = 10;       //默认每10秒执行一次入库操作

    public timerFlushDb() {

    }

    public timerFlushDb(Integer timeout) {
        _timeout = timeout;
    }

    public boolean ifItsTime2FlushDb(String flagStr) {
        if (ifMultiReachTimeout(_timeout, flagStr)) {
            return true;
        }
        return false;
    }

}
