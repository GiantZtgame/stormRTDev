package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import storm.qule_util.JdbcMysql;
import storm.qule_util.cfgLoader;
import storm.qule_util.date;
import storm.qule_util.timerCfgLoader;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;


public class AdRealtimeMysqlBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;
    private Queue<String> sqls = new ConcurrentLinkedQueue<String>();
    private int MAX_QUEUE_SQLS = 2;
    private Set<String> TBS = new HashSet<String>();
    private Long _lastTime = null;

    /**
     * 加载配置文件
     * @param stormConf
     * @param context
     */
    public void prepare(Map stormConf, TopologyContext context) {
        _gamecfg = stormConf.get("gamecfg_path").toString();
        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));
    }
    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String tablename = tuple.getStringByField("tablename");
        String sql_5m = tuple.getStringByField("sql_5m");
        String sql_1h = tuple.getStringByField("sql_1h");

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (null != host) {
            String checkTable = "CREATE TABLE IF NOT EXISTS `" + db + "`.`" + tablename + "`(`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`adplanning_id` int(11) unsigned NOT NULL COMMENT '主线ID',`chunion_subid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '网盟子渠道ID，没有即为0',`datetime` int(11) unsigned NOT NULL COMMENT '时间点，默认5分钟间隔',`duration_type` tinyint(3) unsigned NOT NULL COMMENT '间隔类型：1:5分钟, 2:1小时',`p_reg` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '该时间段内的平台注册',`characters` int(11) unsigned NOT NULL COMMENT '该时间段内的激活',`ip` int(11) unsigned NOT NULL COMMENT '该时间段内的独立IP',`pv` int(11) unsigned NOT NULL COMMENT '该时间段内的PV',PRIMARY KEY (`id`),UNIQUE KEY `adplanning_id` (`adplanning_id`,`chunion_subid`,`datetime`,`duration_type`)) ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='广告实时数据';";
            if (!TBS.contains(tablename)) {
                TBS.clear();
                sqls.add(checkTable);
                TBS.add(tablename);
            }
            sqls.add(sql_5m);
            boolean QUEUE_OFFER = sqls.offer(sql_1h);

            Long currentTime = System.currentTimeMillis() / 1000;
            if (_lastTime==null || currentTime >= _lastTime + 100 || sqls.size() > MAX_QUEUE_SQLS || !QUEUE_OFFER) {
                _lastTime = currentTime;
                JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
                con.batchAdd(sqls);
                System.out.println(" AdRealtime Success ***********");
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}