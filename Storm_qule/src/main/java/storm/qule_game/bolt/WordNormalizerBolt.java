package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.qule_game.GRechargeTopology;
import storm.qule_util.JdbcMysql;
import storm.qule_util.jedisUtil;
import storm.qule_util.md5;
import redis.clients.jedis.Jedis;

import java.util.Properties;

public class WordNormalizerBolt extends BaseRichBolt{
    private static Properties _prop = new Properties();
    private static Jedis _jedis;

    private OutputCollector _collector;
    public void cleanup(){}
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        _collector=collector;
        String conf;
        if (stormConf.get("isOnline").toString().equals("true")) {
            conf = "/config/games.properties";
        }
        else {
            conf = "/config/test.games.properties";
        }
        InputStream game_cfg_in = getClass().getResourceAsStream(conf);
        try {
            _prop.load(game_cfg_in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void execute(Tuple input) {
        String sentence = input.getString(0);
        String[] words = sentence.split("\\|");
        if (words.length == 11) {
            String game_abbr = words[0];
            String platform = words[1];
            String server = words[2];
            String token = words[3];
            String order_id = words[7];
            int status = 1;
            String uname = words[9];
            int ad = 0;
            String cname = words[10];
            String amount = words[8];
            String yb_amnt = words[8];
            String datetime = words[6];

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform + server + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            String sql = "INSERT INTO `recharge_order_today` (`platform`, `server`," +
                    " `order_id`, `status`, `uname`, `ad`, `cname`, `amount`, `yb_amnt`, `datetime`," +
                    " `chid`, `chposid`, `adplanning_id`, `chunion_subid`) VALUES (" + platform + ", " + server + ", '" +
                    order_id + "', " + status + ", '" + uname + "', " + ad + ", '" + cname + "', " + amount + ", " + yb_amnt + "," + datetime +
                    ", 0, 0, 0, 0 );";
            System.out.println("======================================");
            System.out.println("\r\n"+"登录账号："+uname+"\r\n");
            System.out.println("\r\n"+"充值元宝："+yb_amnt+"\r\n");
            System.out.println("\r\n"+"游戏名称："+game_abbr+"\r\n");
            System.out.println("======================================");
            _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
            String key = "grechargeinfo-"+platform+"-"+game_abbr+"-"+server+"-"+uname;
            String value = "grechargedetail-"+platform+"-"+game_abbr+"-"+server+"-"+uname+"-"+datetime;
            _jedis.lpush(key,value);
            Map map = new HashMap();
            map.put("cname",cname);
            map.put("amount",amount);
            map.put("order_id",order_id);
            map.put("log_time",datetime);
            _jedis.hmset(value,map);

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
            JdbcMysql con = JdbcMysql.getInstance(game_abbr,host , port , db, user, passwd);

//            if (token_gen.equals(token)) {
//                JdbcMysql con = JdbcMysql.getInstance(GmRechargeTopology.isOnline,game_abbr);
//                if (con.add(sql) == 1) {
//                    Jedis jedis = new Jedis(_prop.getProperty("redis.host"),6379);
//                    String key = "grechargeinfo-"+platform+"-"+game_abbr+"-"+server+"-"+uname;
//                    String value = "grechargedetail-"+platform+"-"+game_abbr+"-"+server+"-"+uname+"-"+datetime;
//                    jedis.lpush(key,value);
//                    Map map = new HashMap();
//                    map.put("cname",cname);
//                    map.put("amount",amount);
//                    map.put("order_id",order_id);
//                    map.put("log_time",datetime);
//                    jedis.hmset(value,map);
//                    System.out.println("\r\nSuccess\r\n");
//                }
//            }

        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word"));}
}