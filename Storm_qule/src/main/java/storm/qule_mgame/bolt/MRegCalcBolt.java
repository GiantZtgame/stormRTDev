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
 * Created by wangxufeng on 2014/12/2.
 */
public class MRegCalcBolt extends BaseBasicBolt {
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
        String reg_datetime = tuple.getStringByField("reg_datetime");
        String devid = tuple.getStringByField("devid");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String system = tuple.getStringByField("system");
        String appver = tuple.getStringByField("appver");
        String model = tuple.getStringByField("model");
        String resolution = tuple.getStringByField("resolution");
        String sp = tuple.getStringByField("sp");
        String network = tuple.getStringByField("network");
        String client_ip = tuple.getStringByField("client_ip");
        String district = tuple.getStringByField("district");
        String osver = tuple.getStringByField("osver");
        String osbuilder = tuple.getStringByField("osbuilder");
        String devtype = tuple.getStringByField("devtype");

        Long reg_datetime_int = Long.parseLong(reg_datetime);
        String todayStr = date.timestamp2str(reg_datetime_int, "yyyyMMdd");

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

        Integer client = system2client.turnSystem2ClientId(system);

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;
            String sqls = "";

            String dmlsql_reglist = "";
            String inssql_reglist = "";

            String reglist_tb = "reglist_" + todayStr;
            dmlsql_reglist = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`account` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`cname` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`datetime` int(11) unsigned NOT NULL," +
                    "`devid` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`os` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`appver` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`model` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`resolution` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`operator` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`network` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`clientip` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`district` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`osver` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`osbuilder` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`devtype` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "PRIMARY KEY (`id`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", reglist_tb);
            inssql_reglist = String.format("INSERT INTO %s (client, platform, server, version, account, cname, " +
                            "datetime, devid, os, appver, model, resolution, operator, network, clientip, district," +
                            "osver, osbuilder, devtype) VALUES (%d, '%s', '%s', '%s', '%s', '%s', %d, '%s', '%s', '%s', " +
                            "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');", reglist_tb, client,
                    platform_id, server_id, appver, uname, cname, reg_datetime_int, devid, system, appver, model,
                    resolution, sp, network, client_ip, district, osver, osbuilder, devtype);

            sqls += dmlsql_reglist;
            sqls += inssql_reglist;

System.out.println(sqls);
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