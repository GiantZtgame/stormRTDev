package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_game.GDailyRemainTopology;
import storm.qule_game.GRechargeTopology;
import storm.qule_util.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by zhanghang on 2014/7/22.
 */
public class GDailyRemainBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (stormConf.get("isOnline").toString().equals("true")) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //[AHSG, 100, 1, 1385339688, 1, 59.51.56.166, mact2013, 119.97.171.104, 6020]
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        Long logtime = tuple.getLongByField("login_datetime");
        String uname = tuple.getStringByField("uname");

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
        if (host != null) {
            String todayStr = date.timestamp2str(logtime, "yyyyMMdd");
            List someday =  Arrays.asList(1,2,3,4,5,6,7,14,30);
            List<String> sqls = new ArrayList<String>();
            System.out.println("======================================");
            for (int i = 0; i < someday.size(); i++) {
                String day = someday.get(i).toString();
                int d = Integer.parseInt(todayStr) - Integer.parseInt(day);
                Long datestamp = date.str2timestamp(d+" 00:00:00");

                //===============================一般用户================================
                //redis key
                String someday_char = "login:" + platform_id + ":" + server_id + ":" + game_abbr + ":" + d + ":char:set";
                String dailyremain = "gdailyremain:" + platform_id + ":" + server_id + ":" + game_abbr + ":" + todayStr + ":" + i + ":incr";
                if (_jedis.sismember(someday_char, uname)) {
                    _jedis.incr(dailyremain);
                    _jedis.expire(dailyremain, 24 * 60 * 60);
                }
                String data = _jedis.exists(dailyremain) ? _jedis.get(dailyremain) : "0";
                String sql = "INSERT INTO `opdata_dailyRemain` (`platform`,`server`,`date`,`day" + day + "`) VALUES(" + platform_id + "," + server_id + "," + datestamp + "," + data +
                        ") ON DUPLICATE KEY UPDATE `day" + day + "` = " + data;
                sqls.add(sql);
                //===============================广告用户================================
                String hash_key = "gadinfo-" + platform_id + "-" + uname;
                if (_jedis.exists(hash_key)) {
                    List<String> gadinfo = _jedis.hmget(hash_key, "chid", "chposid", "adplanning_id", "chunion_subid");
                    String adplanning_id = gadinfo.get(2);
                    String chunion_subid = gadinfo.get(3);
                    if (Integer.parseInt(adplanning_id) > 0) {
                        String adsql = "INSERT INTO `adplanning_dailyremain` (`adplanning_id`,`chunion_subid`,`platform`,`server`,`date`,`day" + day + "`) VALUES(" + adplanning_id + "," + chunion_subid + ","+platform_id + "," + server_id + "," + datestamp + "," + data +
                                ") ON DUPLICATE KEY UPDATE `day" + day + "` = " + data;
                        sqls.add(adsql);
                    }
                }
                System.out.println("第" + day + "日留存：" + data);
            }
            System.out.println("======================================");
            JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
            if (con.batchAdd(sqls)) {
                System.out.println("*********** Success ************");
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform_id","server_id","datetime","up_time","mj_characters","js_characters","ds_characters","daily_logins","daily_logins_char","daily_logins_new","daily_logins_char_new","daily_characters"));
    }
}
