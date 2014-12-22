package storm.qule_mgame.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;
import storm.qule_util.mgame.system2client;

import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/12/1.
 */
public class MObjCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();
    private static mysql _dbconnect = null;

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static timerFlushDb _dbFlushTimer = new timerFlushDb();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    /**
     * 加载配置文件
     *
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
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")), 11);

        _dbconnect = new mysql();

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        String obj_datetime = tuple.getStringByField("obj_datetime");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("uname");
        String system = tuple.getStringByField("system");
        String level = tuple.getStringByField("level");
        String objname = tuple.getStringByField("objname");
        String objcateid = tuple.getStringByField("objcateid");
        String objid = tuple.getStringByField("objid");
        String ifincr = tuple.getStringByField("ifincr");
        String ifbind = tuple.getStringByField("ifbind");
        String amount = tuple.getStringByField("amount");
        String original_amount = tuple.getStringByField("original_amount");
        String opname = tuple.getStringByField("opname");

        Long obj_datetime_int = Long.parseLong(obj_datetime);
        String obj_datetime_str = date.timestamp2str(obj_datetime_int, "yyyyMMdd");

        Long obj_datetime_date = date.str2timestamp(obj_datetime_str, "yyyyMMdd");

        Integer client = system2client.turnSystem2ClientId(system);

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)
                && _dbFlushTimer.ifItsTime2FlushDb(client.toString()+platform_id+server_id)) {
            boolean sql_ret = false;
            String sqls = "";

            String obj_tb = "obj_" + platform_id + "_" + server_id;
            String dmlsql_obj_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`datetime` int(11) unsigned NOT NULL," +
                    "`uname` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`cname` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`level` mediumint(5) unsigned NOT NULL DEFAULT 0," +
                    "`objname` char(255) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`objcateid` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`objid` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`ifincr` tinyint(2) unsigned NOT NULL DEFAULT 0," +
                    "`ifbind` tinyint(2) unsigned NOT NULL DEFAULT 0," +
                    "`amount` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`original_amount` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`opname` char(255) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "PRIMARY KEY (`id`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", obj_tb);
            String inssql_obj = String.format("INSERT INTO %s (client, platform, server, datetime, uname, cname, level, " +
                    "objname, objcateid, objid, ifincr, ifbind, amount, original_amount, opname) VALUES (%d, '%s', '%s'," +
                    " %d, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", obj_tb, client,
                    platform_id, server_id, obj_datetime_int, uname, cname, level, objname, objcateid, objid, ifincr,
                    ifbind, amount, original_amount, opname);

            sqls += dmlsql_obj_tb;
            sqls += inssql_obj;
System.out.println("------------" + sqls);
            try {
                sql_ret = _dbconnect.DirectUpdateBatch(game_abbr, sqls);
            } catch (SQLException e) {
                e.printStackTrace();
            }
System.out.println("|||||-----batch update result: " + sql_ret);

        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

}