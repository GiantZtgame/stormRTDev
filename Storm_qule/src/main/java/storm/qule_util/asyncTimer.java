package storm.qule_util;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by wangxufeng on 2014/8/11.
 */
public class asyncTimer {
    //缓存刷新时间(秒)
    private int _cache_timeout = 60;

    private Long _last_load_time_sec = 0L;

    private Map<String, Long> _map_last_load_time_sec = new HashMap<String, Long>();

    public asyncTimer() {
        this(60);
    }

    public asyncTimer(int cache_timeout) {
        _cache_timeout = cache_timeout;
    }

    public boolean ifReachTimeout() {
        return ifReachTimeout(_cache_timeout);
//        if (0 == _last_load_time_sec) {
//            _last_load_time_sec = System.currentTimeMillis()/1000;
//        }
//
//        Long cur_timestamp = System.currentTimeMillis()/1000;
//
//        if ( (cur_timestamp - _last_load_time_sec) >= _cache_timeout ) {
//            _last_load_time_sec = cur_timestamp;
//            return true;
//        } else {
//            return false;
//        }
    }

    public boolean ifReachTimeout(Integer timeout) {
        _cache_timeout = timeout;

        if (0 == _last_load_time_sec) {
            _last_load_time_sec = System.currentTimeMillis()/1000;
        }

        Long cur_timestamp = System.currentTimeMillis()/1000;

        if ( (cur_timestamp - _last_load_time_sec) >= _cache_timeout ) {
            _last_load_time_sec = cur_timestamp;
            return true;
        } else {
            return false;
        }
    }


    public boolean ifMultiReachTimeout(Integer timeout, String flagStr) {
        Long last_load_time_sec = 0L;
        if (_map_last_load_time_sec.containsKey(flagStr)) {
            last_load_time_sec = _map_last_load_time_sec.get(flagStr);
        } else {
            last_load_time_sec = System.currentTimeMillis()/1000;
            _map_last_load_time_sec.put(flagStr, last_load_time_sec);
        }

        Long cur_timestamp = System.currentTimeMillis()/1000;
        if ( (cur_timestamp - last_load_time_sec) >= timeout ) {
            _map_last_load_time_sec.put(flagStr, cur_timestamp);
            return true;
        } else {
            return false;
        }
    }
}
