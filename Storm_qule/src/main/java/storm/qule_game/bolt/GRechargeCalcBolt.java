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
import redis.clients.jedis.Jedis;
import storm.qule_util.*;

import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class GRechargeCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    /**
     * 加载配置文件
     * @param stormConf
     * @param context
     */
    public void prepare(Map stormConf, TopologyContext context) {
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
        if (isOnline) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //[AHSG, 100, 1, 131125-02:19:43, 1385212559, JW201311241109190353, 100, qqq18986173900, 战天无悔＿]
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform = tuple.getStringByField("platform");
        String server = tuple.getStringByField("server");
        String logtime = tuple.getStringByField("logtime");
        String order_id = tuple.getStringByField("order_id");
        String amount = tuple.getStringByField("amount");
        String yb_amnt = tuple.getStringByField("amount");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String level = tuple.getStringByField("level");
        String jid = tuple.getStringByField("jid");

        String adkey = "gadinfo-" + platform + "-" + uname;
        String chid = "0";
        String chposid = "0";
        String adplanning_id = "0";
        String chunion_subid = "0";
        if (_jedis.exists(adkey)) {
            List<String> gadinfo = _jedis.hmget(adkey, "chid", "chposid", "adplanning_id", "chunion_subid");
            chid = gadinfo.get(0);
            chposid = gadinfo.get(1);
            adplanning_id = gadinfo.get(2);
            chunion_subid = gadinfo.get(3);
        }
        System.out.println("=================================");
        System.out.println("平台号：" + platform);
        System.out.println("区号：" + server);
        System.out.println("账号：" + uname);
        System.out.println("游戏名：" + cname);
        System.out.println("充值订单：" + order_id);
        System.out.println("充值元宝：" + yb_amnt);
        System.out.println("等级：" + level);
        System.out.println("职业ID：" + jid);
        System.out.println("=================================");
        String key = "grechargeinfo-" + platform + "-" + game_abbr + "-" + server + "-" + uname;
        String value = "grechargedetail-" + platform + "-" + game_abbr + "-" + server + "-" + uname + "-" + logtime;
        _jedis.rpush(key, value);
        Map map = new HashMap();
        map.put("cname", cname);
        map.put("amount", amount);
        map.put("order_id", order_id);
        map.put("log_time", logtime);
        map.put("lv", level);
        map.put("jid", jid);
        _jedis.hmset(value, map);
        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
        String sql = "INSERT INTO `recharge_order_today` (`platform`, `server`,`order_id`, `status`, `uname`, `ad`, `cname`," +
                " `amount`, `yb_amnt`, `datetime`,`chid`, `chposid`, `adplanning_id`, `chunion_subid`, `lv`, `jid`) VALUES (" +
                platform + ", " + server + ", '" +order_id + "', 1, '" + uname + "', 0, '" + cname + "', " + amount + ", " +
                yb_amnt + "," + logtime +", "+chid+", "+chposid+","+adplanning_id+", "+chunion_subid+", " +
                Integer.parseInt(level) + ", " + Integer.parseInt(jid) + " );";
System.out.println(sql);
        if (con.add(sql)) {
            System.out.println("*************Success**************");
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}