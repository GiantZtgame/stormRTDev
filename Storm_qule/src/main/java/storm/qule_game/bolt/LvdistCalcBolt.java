package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.qule_util.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LvdistCalcBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    private static final String LVDIST_SIGN = "lvdist";
    private static final String VIPLVDIST_SIGN = "viplvdist";
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
    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|2013-11-25|1:1024;2:800;3:500;
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform = tuple.getStringByField("platform");
        String server = tuple.getStringByField("server");
        String todayStr = tuple.getStringByField("todayStr");
        String[] lvdists = tuple.getStringByField("lvdists").split(";");
        String keywords = tuple.getStringByField("keywords");

        System.out.println("============="+platform + ":" + server + ":" + game_abbr+":lvdist==============");

        //datetime 当天时间戳
        Long datetime = date.str2timestamp(todayStr);
        List<String> sqls = new ArrayList<String>();

        String lvDbCol = "";
        if (LVDIST_SIGN.equals(keywords))
            lvDbCol = "level";
        else if (VIPLVDIST_SIGN.equals(keywords))
            lvDbCol = "viplv";

        for (String list : lvdists) {
            String[] data = list.split(":");
            if (data.length == 2) {
                String level = data[0];
                String num = data[1];

                String sql = "INSERT INTO `opdata_lvDist` (`platform`, `server`," +
                        " `date`, `" + lvDbCol + "`, `num`) VALUES (" + platform + ", " +
                        server + ", " + datetime + "," + level + ", " + num + " ) ON DUPLICATE KEY UPDATE " +
                        "`" + lvDbCol + "`=" + level + ",`num`=" + num;
                sqls.add(sql);

                System.out.println(lvDbCol + " " + level + "级人数：" + num);
            }
        }
        System.out.println("======================================");

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
        JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

        con.batchAdd(sqls);
        System.out.println("*********** Success ************");
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}