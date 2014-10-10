package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import java.security.NoSuchAlgorithmException;
import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.qule_util.*;
import redis.clients.jedis.Jedis;

public class GRechargeBolt extends BaseBasicBolt {
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

    /**
     * @param input
     */
    public void execute(Tuple input, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|131125-02:19:43|bill|1385212559|JW201311241109190353|100|qqq18986173900|战天无悔＿[|80|1]
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length == 11 || logs.length == 13) {
            String game_abbr = logs[0];
            String platform = logs[1];
            String server = logs[2];
            String token = logs[3];
            String keywords = logs[5];
            String datetime = logs[6];
            String order_id = logs[7];
            String amount = logs[8];
            String yb_amnt = logs[8];
            String uname = logs[9];
            String cname = logs[10];
            String level = "";
            String jid = "";
            try {
                level = logs[11];
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            try {
                jid = logs[12];
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
            if (host != null) {

                if (keywords.equals("bill")) {
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
                    String log_key = _prop.getProperty("game." + game_abbr + ".key");
                    String raw_str = game_abbr + platform + server + log_key;
                    String token_gen = "";
                    try {
                        token_gen = new md5().gen_md5(raw_str);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    if (token_gen.equals(token)) {
                        JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
                        String tbname = "recharge_order_today";
                        final Map<String, Object> insert = new HashMap<String, Object>();
                        insert.put("platform", platform);
                        insert.put("server", server);
                        insert.put("order_id", order_id);
                        insert.put("status", 1);
                        insert.put("uname", uname);
                        insert.put("ad", 0);
                        insert.put("cname", cname);
                        insert.put("amount", amount);
                        insert.put("yb_amnt", yb_amnt);
                        insert.put("datetime", datetime);
                        insert.put("chid", chid);
                        insert.put("chposid", chposid);
                        insert.put("adplanning_id", adplanning_id);
                        insert.put("chunion_subid", chunion_subid);
                        if (!"".equals(level)) {
                            insert.put("lv", level);
                        }
                        if (!"".equals(jid)) {
                            insert.put("jid", jid);
                        }

                        Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>() {{
                            put("insert", insert);
                        }};
                        String sql = con.setSql("insert", tbname, data);

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
                        if (con.add(sql)) {
                            String key = "grechargeinfo-" + platform + "-" + game_abbr + "-" + server + "-" + uname;
                            String value = "grechargedetail-" + platform + "-" + game_abbr + "-" + server + "-" + uname + "-" + datetime;
                            _jedis.rpush(key, value);
                            Map map = new HashMap();
                            map.put("cname", cname);
                            map.put("amount", amount);
                            map.put("order_id", order_id);
                            map.put("log_time", datetime);
                            map.put("lv", level);
                            map.put("jid", jid);
                            _jedis.hmset(value, map);
                            System.out.println("*************Success**************");
                        }
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word"));}
}