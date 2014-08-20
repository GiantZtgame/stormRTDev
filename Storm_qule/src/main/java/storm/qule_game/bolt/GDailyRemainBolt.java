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
    private static Jedis _jedis;
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
        String game_abbr = tuple.getString(0);
        String platform_id = tuple.getString(1);
        String server_id = tuple.getString(2);
        Long datetime = tuple.getLong(3);
        String login_success = tuple.getString(4);
        String login_ip = tuple.getString(5);
        String uname = tuple.getString(6);

        if (login_success.equals("1")) {
            //判断是否为广告用户
            String tbname = "opdata_dailyRemain";
            String hash_key = "gadinfo-" + platform_id + "-" + uname;
            if (_jedis.exists(hash_key)) {
                String adplanning_id = _jedis.hget(hash_key, "adplanning_id");
                if (Integer.parseInt(adplanning_id) > 0) {
                    tbname = "adthru_dailyRemain";
                }
            }
            String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
            List someday = new ArrayList();
            List<String> sqls = new ArrayList<String>();
            someday.add(1);
            someday.add(2);
            someday.add(3);
            someday.add(4);
            someday.add(5);
            someday.add(6);
            someday.add(7);
            someday.add(14);
            someday.add(30);
            System.out.println("======================================");
            for (int i=0;i<someday.size();i++) {
                String day = someday.get(i).toString();
                int d = Integer.parseInt(todayStr)-Integer.parseInt(day);
                //redis key
                String someday_ip = "login:"+platform_id+":"+server_id+":"+game_abbr+":"+d+":ip:set";
                String dailyremain = "gdailyremain:"+platform_id+":"+server_id+":"+game_abbr+":"+todayStr+":"+i+":incr";
                if (_jedis.sismember(someday_ip, login_ip)) {
                    _jedis.incr(dailyremain);
                    _jedis.expire(dailyremain, 24*60*60);
                }
                String data = _jedis.exists(dailyremain)?_jedis.get(dailyremain):"0";
                String sql = "INSERT INTO `"+tbname+"` (`platform`,`server`,`date`,`day"+day+"`) VALUES("+platform_id+","+server_id+","+d+","+data+
                        ") ON DUPLICATE KEY UPDATE `day"+day+"` = "+data;
                sqls.add(sql);
                System.out.println("第"+day+"日留存："+data);
            }
            System.out.println("======================================");

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

            JdbcMysql con = JdbcMysql.getInstance(game_abbr,host , port , db, user, passwd);
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
