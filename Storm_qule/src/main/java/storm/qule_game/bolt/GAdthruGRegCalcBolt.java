package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.qule_util.cfgLoader;
import storm.qule_util.date;
import storm.qule_util.jedisUtil;
import storm.qule_util.timerCfgLoader;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/7/30.
 */
public class GAdthruGRegCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

//    Map<String, Integer> todayAdthruRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruHourlyRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruJoblyRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruIpList = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruIplyRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruDurationRegCounts = new HashMap<String, Integer>();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        String conf;
        if (stormConf.get("isOnline").toString().equals("true")) {
            //conf = "/config/games.properties";
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

//        InputStream redis_cfg_in = getClass().getResourceAsStream(_gamecfg);
//
//        try {
//            _prop.load(redis_cfg_in);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String game_abbr = tuple.getString(0);
        String platform_id = tuple.getString(1);
        String server_id = tuple.getString(2);
        Long datetime = tuple.getLong(3);
        String reg_success = tuple.getString(4);
        String uname = tuple.getString(5);
        String cname = tuple.getString(6);
        String job_id = tuple.getString(7);
        String client_ip = tuple.getString(8);

        if ("1".equals(reg_success)) {
            //获取广告信息
            String hash_key = "gadinfo-" + platform_id + "-" + uname;
            if (_jedis.exists(hash_key)) {
                List<String> gadinfo = _jedis.hmget(hash_key, "chid", "chposid", "adplanning_id", "chunion_subid");
                String chid = gadinfo.get(0);
                String chposid = gadinfo.get(1);
                String adplanning_id = gadinfo.get(2);
                String chunion_subid = gadinfo.get(3);

                if (Integer.parseInt(adplanning_id) > 0) {
                    //当天总广告注册人数
                    String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
                    Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

                    String key1 = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + adplanning_id + ":" + chunion_subid + ":incr";
                    //Integer count1  = todayAdthruRegCounts.get(key1);
                    Integer count1 = !_jedis.exists("adregChar:" + key1) ? 0 : Integer.parseInt(_jedis.get("adregChar:" + key1));
                    //if (null == count1) {
                    //    count1 = 0;
                    //}
                    count1++;
                    _jedis.set("adregChar:" + key1, count1.toString());
                    _jedis.expire("adregChar:" + key1, 24*60*60);
                    //todayAdthruRegCounts.put(key1, count1);

                    //当天总广告注册IP数
                    String ipKey = todayStr + "-" + game_abbr + "-" + platform_id + "-" + server_id + "-" + client_ip + "-" + adplanning_id + "-" + chunion_subid;
                    //Integer countip = todayAdthruIplyRegCounts.get(key1);
                    Integer countip = !_jedis.exists("adregIp:" + key1) ? 0 : Integer.parseInt(_jedis.get("adregIp:" + key1));
                    //if (null == countip) {
                    //    countip = 0;
                    //}
                    //if (!todayAdthruIpList.containsKey(ipKey)) {
                    //    todayAdthruIpList.put(ipKey, 1);
                    //    countip++;
                    //    todayAdthruIplyRegCounts.put(key1, countip);
                    //}
                    if (!_jedis.sismember("adregIpList:" + todayStr + ":set", ipKey)) {
                        countip++;
                        _jedis.sadd("adregIpList:" + todayStr + ":set", ipKey);
                        _jedis.expire("adregIpList:" + todayStr + ":set", 30*24*60*60);
                        _jedis.set("adregIp:" + key1, countip.toString());
                        _jedis.expire("adregIp:" + key1, 24*60*60);
                    }

                    //当天按时段广告注册人数
                    String todayHourStr = date.timestamp2str(datetime, "yyyyMMdd-HH");
                    String hour = date.timestamp2str(datetime, "H");
                    String key2 = todayHourStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + adplanning_id + ":" + chunion_subid + ":incr";
                    //Integer count2 = todayAdthruHourlyRegCounts.get(key2);
                    Integer count2 = !_jedis.exists("adHourlyRegCounts:" + key2) ? 0 : Integer.parseInt(_jedis.get("adHourlyRegCounts:" + key2));
                    //if (null == count2) {
                    //    count2 = 0;
                    //}
                    count2++;
                    //todayAdthruHourlyRegCounts.put(key2, count2);
                    _jedis.set("adHourlyRegCounts:" + key2, count2.toString());
                    _jedis.expire("adHourlyRegCounts:" + key2, 24*60*60);

                    //当天按职业广告注册人数
                    String key3 = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + job_id + ":" + adplanning_id + ":" + chunion_subid + ":incr";
                    //Integer count3 = todayAdthruJoblyRegCounts.get(key3);
                    Integer count3 = !_jedis.exists("adJoblyRegCounts:" + key3) ? 0 : Integer.parseInt(_jedis.get("adJoblyRegCounts:" + key3));
                    //if (null == count3) {
                    //    count3 = 0;
                    //}
                    count3++;
                    //todayAdthruJoblyRegCounts.put(key3, count3);
                    _jedis.set("adJoblyRegCounts:" + key3, count3.toString());
                    _jedis.expire("adJoblyRegCounts:" + key3, 24*60*60);

                    //每5分钟渠道激活
                    int duration = 300;
                    Long tis_datetime_5min = datetime - datetime % duration;
                    String key5min = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + adplanning_id + ":" + chunion_subid + ":5min:incr";
                    //Integer count5min = todayAdthruDurationRegCounts.get(key5min);
                    Integer count5min = !_jedis.exists("adDurationRegCounts:" + key5min) ? 0 : Integer.parseInt(_jedis.get("adDurationRegCounts:" + key5min));
                    //if (null == count5min) {
                    //    count5min = 0;
                    //}
                    count5min++;
                    //todayAdthruDurationRegCounts.put(key5min, count5min);
                    _jedis.set("adDurationRegCounts:" + key5min, count5min.toString());
                    _jedis.expire("adDurationRegCounts:" + key5min, 24*60*60);

                    //每小时渠道激活
                    duration = 3600;
                    Long tis_datetime_1hour = datetime - datetime % duration;
                    String key1hour = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + adplanning_id + ":" + chunion_subid + ":1hour:incr";
                    //Integer count1hour = todayAdthruDurationRegCounts.get(key1hour);
                    Integer count1hour = !_jedis.exists("adDurationRegCounts:" + key1hour) ? 0 : Integer.parseInt(_jedis.get("adDurationRegCounts:" + key1hour));
                    //if (null == count1hour) {
                    //    count1hour = 0;
                    //}
                    count1hour++;
                    //todayAdthruDurationRegCounts.put(key1hour, count1hour);
                    _jedis.set("adDurationRegCounts:" + key1hour, count1hour.toString());
                    _jedis.expire("adDurationRegCounts:" + key1hour, 24*60*60);

                    collector.emit(new Values(game_abbr, platform_id, server_id, todayDate, datetime, count1, countip, hour, count2, job_id, count3, adplanning_id,
                            chunion_subid, count5min, tis_datetime_5min, count1hour, tis_datetime_1hour, todayStr));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "todayDate", "datetime", "count1", "countip", "hour", "count2", "job_id",
                "count3", "adplanning_id", "chunion_subid", "count5min", "tis_datetime_5min", "count1hour", "tis_datetime_1hour", "todayStr"));
    }
}
