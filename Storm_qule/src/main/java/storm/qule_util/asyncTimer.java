package storm.qule_util;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/8/11.
 */
public class asyncTimer {
    //缓存刷新时间(秒)
    private int _cache_timeout = 60;

    private Long _last_load_time_sec = 0L;

    public asyncTimer() {
        this(60);
    }

    public asyncTimer(int cache_timeout) {
        _cache_timeout = cache_timeout;
    }

    //check if reach time point
    public boolean ifReachTimeout() {
        if (0 == _last_load_time_sec) {
            _last_load_time_sec = System.currentTimeMillis()/1000;
        }

        Long cur_timestamp = System.currentTimeMillis()/1000;
//System.out.println("1********************************************************" + cur_timestamp);
//System.out.println("2********************************************************" + _last_load_time_sec);
        if ( (cur_timestamp - _last_load_time_sec) >= _cache_timeout ) {
            _last_load_time_sec = cur_timestamp;
            return true;
        } else {
            return false;
        }
    }
}
