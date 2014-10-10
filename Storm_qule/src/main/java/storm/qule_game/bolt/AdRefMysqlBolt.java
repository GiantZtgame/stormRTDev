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

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

public class AdRefMysqlBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;
    private Queue<String> sqls = new ConcurrentLinkedQueue<String>();
    private int MAX_QUEUE_SQLS = 400;
    private Long _lastTime = null;

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
    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);
        //"game_abbr","sql"
        String game_abbr = tuple.getStringByField("game_abbr");
        String sql = tuple.getStringByField("sql");

        String HOST = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String PORT = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String DB = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String USER = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String PASSWD = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (null != HOST) {
            boolean QUEUE_OFFER = sqls.offer(sql);

            Long currentTime = System.currentTimeMillis() / 1000;
            if (_lastTime==null || currentTime >= _lastTime + 150 || sqls.size() > MAX_QUEUE_SQLS || !QUEUE_OFFER) {
                _lastTime = currentTime;
                JdbcMysql con = JdbcMysql.getInstance(game_abbr,HOST,PORT,DB,USER,PASSWD);
                con.batchAdd(sqls);
                System.out.println("********* AdRef Success ***********");
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}