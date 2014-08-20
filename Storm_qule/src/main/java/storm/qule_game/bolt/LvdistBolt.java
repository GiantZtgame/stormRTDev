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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LvdistBolt extends BaseBasicBolt {
    private static Jedis _jedis;
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
        if (Boolean.parseBoolean(stormConf.get("isOnline").toString())) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple input , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|xxxxx|2013-11-25 08:34:48|lvdist|1:1024;2:800;3:500
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length == 7) {
            String game_abbr = logs[0];
            String platform = logs[1];
            String server = logs[2];
            String token = logs[3];
            String datestr = logs[4];
            String keywords = logs[5];
            String[] lvdists = logs[6].split(";");

            //验证token
            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform + server + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if (token_gen.equals(token)) {

                //redis key
                String lvdist_key = "lvdist:" + platform + ":" + server + ":" + game_abbr + ":hash";

                System.out.println("===============等级分布==============");
                for (String list : lvdists) {
                    String[] data = list.split(":");
                    if (data.length == 2) {
                        String level = data[0];
                        String num = data[1];
                        _jedis.hset(lvdist_key, level, num);
                        System.out.println(level + "级人数：" + num);
                    }
                }
                System.out.println("======================================");
                //五分钟记录一次数据库
                if (!_jedis.exists("timer:lvdist:5m")) {

                    String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
                    String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
                    String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
                    String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
                    String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

                    JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

                    List<String> sqls = new ArrayList<String>();
                    Map<String, String> maps = _jedis.hgetAll(lvdist_key);

                    //date 当天0点时间戳
                    String[] date1 = datestr.split(" ");
                    if (date1.length == 2) {
                        datestr = date1[0]+" 00:00:00";
                    }
                    Long datetime = date.str2timestamp(datestr);

                    for (Map.Entry entry : maps.entrySet()) {
                        String sql = "INSERT INTO `opdata_lvDist` (`platform`, `server`," +
                                " `date`, `level`, `num`) VALUES (" + platform + ", " +
                                server + ", " + datetime + "," + entry.getKey() + ", " + entry.getValue() + " ) ON DUPLICATE KEY UPDATE " +
                                "`level`=" + entry.getKey() + ",`num`=" + entry.getValue();
                        sqls.add(sql);
                    }
                    if (con.batchAdd(sqls)) {
                        System.out.println("*********** Success ************");
                        _jedis.setex("timer:lvdist:5m", 5 * 60, "1");
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}