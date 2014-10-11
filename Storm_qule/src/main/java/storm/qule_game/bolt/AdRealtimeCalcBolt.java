package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twitter.chill.java.ArraysAsListSerializer;
import redis.clients.jedis.Jedis;

import storm.qule_util.JdbcMysql;
import storm.qule_util.date;
import storm.qule_util.jedisUtil;

import java.util.*;


public class AdRealtimeCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;

    /**
     * @param conf
     * @param context
     */
    public void prepare(Map conf, TopologyContext context) {
        _jedis = new jedisUtil().getJedis(conf.get("redis.host").toString(), Integer.parseInt(conf.get("redis.port").toString()));
    }
    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //[AHSG, 100, 1, 2013-11-25 08:34:48, adreg, 100, 0, 127.0.0.1, testaccount]
        String game_abbr = tuple.getStringByField("game_abbr");
//        String platform = tuple.getStringByField("platform");
//        String server = tuple.getStringByField("server");
        String logtime = tuple.getStringByField("logtime");
        String keywords = tuple.getStringByField("keywords");
        String adplanning_id = tuple.getStringByField("adplanning_id");
        String chunion_subid = tuple.getStringByField("chunion_subid");
        String ip = tuple.getStringByField("ip");
        String uname = tuple.getStringByField("uname");

        //logtime 2013-11-25 08:34:48
        Long logtimestamp = date.str2timestamp(logtime);
        String todayStr = date.timestamp2str(logtimestamp, "yyyyMMdd");
        Long tis_datetime_5m = logtimestamp - logtimestamp % 300;
        Long tis_datetime_1h = logtimestamp - logtimestamp % 3600;

        String R5M = adplanning_id + ":" +chunion_subid + ":" + tis_datetime_5m;
        String R1H = adplanning_id + ":" +chunion_subid + ":" + tis_datetime_1h;

        //redis key
        String r_adreg_5m = "adreg:" + R5M + ":incr";
        String r_adreg_1h = "adreg:" + R1H + ":incr";
        String r_adex_ip_5m = "adex:" + R5M + ":ip:set";
        String r_adex_ip_1h = "adex:" + R1H + ":ip:set";
        String r_adex_pv_5m = "adex:" + R5M + ":pv:incr";
        String r_adex_pv_1h = "adex:" + R1H + ":pv:incr";
        //注册
        //记录5分钟和1小时时间段注册人数
        if (keywords.equals("adreg")) {
            _jedis.sadd(r_adreg_5m,uname);
            _jedis.sadd(r_adreg_1h,uname);

            _jedis.expire(r_adreg_5m,5*60);
            _jedis.expire(r_adreg_1h,60*60);
        }
        //广告弹出
        //记录5分钟，1小时广告弹出的ip和pv
        if (keywords.equals("adex")) {
            _jedis.sadd(r_adex_ip_5m, ip);
            _jedis.sadd(r_adex_ip_1h, ip);

            _jedis.incr(r_adex_pv_5m);
            _jedis.incr(r_adex_pv_1h);

            _jedis.expire(r_adex_pv_5m,5*60);
            _jedis.expire(r_adex_pv_1h,60*60);
            _jedis.expire(r_adex_ip_5m,5*60);
            _jedis.expire(r_adex_ip_1h,60*60);
        }

        Long adreg_5m = _jedis.scard(r_adreg_5m);
        Long adreg_1h = _jedis.scard(r_adreg_1h);

        String adex_pv_5m = _jedis.exists(r_adex_pv_5m) ? _jedis.get(r_adex_pv_5m) : "0";
        String adex_pv_1h = _jedis.exists(r_adex_pv_1h) ? _jedis.get(r_adex_pv_1h) : "0";

        Long adex_ip_5m = _jedis.scard(r_adex_ip_5m);
        Long adex_ip_1h = _jedis.scard(r_adex_ip_1h);

        String tablename = "ad_realtime_" + todayStr;
        String sql_5m = "INSERT INTO `"+tablename+"` (`adplanning_id`, `chunion_subid`, `datetime`, `duration_type`, `p_reg`, `characters`, `ip`,`pv`) VALUES (" + adplanning_id + ", " + chunion_subid + ", " +tis_datetime_5m + ", 1 , " + adreg_5m + ", 0, " + adex_ip_5m + ", " + adex_pv_5m+ " ) ON DUPLICATE KEY UPDATE `p_reg`="+adreg_5m+",`ip`="+adex_ip_5m+",`pv`="+adex_pv_5m+";";
        String sql_1h = "INSERT INTO `"+tablename+"` (`adplanning_id`, `chunion_subid`,`datetime`, `duration_type`, `p_reg`, `characters`, `ip`,`pv`) VALUES (" + adplanning_id + ", " + chunion_subid + ", " +tis_datetime_1h + ", 2 , " + adreg_1h + ",0, " + adex_ip_1h + ", " + adex_pv_1h+ " ) ON DUPLICATE KEY UPDATE `p_reg`="+adreg_1h+",`ip`="+adex_ip_1h+",`pv`="+adex_pv_1h+";";

        collector.emit(new Values(game_abbr,tablename,sql_5m,sql_1h));
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","tablename","sql_5m","sql_1h"));
    }
}