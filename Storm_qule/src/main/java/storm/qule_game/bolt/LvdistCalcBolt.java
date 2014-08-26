package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LvdistCalcBolt extends BaseBasicBolt {
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
        } else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //"game_abbr", "platform", "server","logtime", "lvdists"
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform = tuple.getStringByField("platform");
        String server = tuple.getStringByField("server");
        String logtime = tuple.getStringByField("logtime");
        String[] lvdists = tuple.getStringByField("lvdists").split(";");

        //redis key
        String PSG = platform + ":" + server + ":" + game_abbr;
        String lvdist_key = "lvdist:" + PSG + ":hash";

        System.out.println("============="+PSG+":lvdist==============");
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

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
        JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
        //date 当天0点时间戳
        String[] date1 = logtime.split(" ");
        if (date1.length == 2) {
            logtime = date1[0] + " 00:00:00";
        }
        Long datetime = date.str2timestamp(logtime);

        List<String> sqls = new ArrayList<String>();
        Map<String, String> maps = _jedis.hgetAll(lvdist_key);
        for (Map.Entry entry : maps.entrySet()) {
            String sql = "INSERT INTO `opdata_lvDist` (`platform`, `server`," +
                    " `date`, `level`, `num`) VALUES (" + platform + ", " +
                    server + ", " + datetime + "," + entry.getKey() + ", " + entry.getValue() + " ) ON DUPLICATE KEY UPDATE " +
                    "`level`=" + entry.getKey() + ",`num`=" + entry.getValue();
            sqls.add(sql);
        }
        if (con.batchAdd(sqls)) {
            System.out.println("*********** Success ************");
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}