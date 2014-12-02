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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/11/24.
 */
public class MStartupCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();
    private static mysql _dbconnect = null;

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

        _dbconnect = new mysql();

    }
    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        String startup_datetime = tuple.getStringByField("startup_datetime");
        String devid = tuple.getStringByField("devid");
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

        Long startup_datetime_int = Long.parseLong(startup_datetime);
        String todayStr = date.timestamp2str(startup_datetime_int, "yyyyMMdd");
        String todayHourStr = date.timestamp2str(startup_datetime_int, "yyyyMMdd-HH");
        String todayMinuteStr = date.timestamp2str(startup_datetime_int, "yyyyMMdd-HHmm");
        String curHour = date.timestamp2str(startup_datetime_int, "H");

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");
        Long todayMinuteDate = date.str2timestamp(todayMinuteStr, "yyyyMMdd-HHmm");

        Integer client = system2client.turnSystem2ClientId(system);


        //记录启动列表
        String mstartupListKey = "mstartup:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + devid + ":record";
        String mstartupListValue = "mstartup:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + devid + ":" +
                startup_datetime + ":record:detail";
        _jedis.rpush(mstartupListKey, mstartupListValue);

        Map mstartupListDetailMap = new HashMap();
        mstartupListDetailMap.put("os", system);
        mstartupListDetailMap.put("appver", appver);
        mstartupListDetailMap.put("model", model);
        mstartupListDetailMap.put("resolution", resolution);
        mstartupListDetailMap.put("operator", sp);
        mstartupListDetailMap.put("network", network);
        mstartupListDetailMap.put("clientip", client_ip);
        mstartupListDetailMap.put("district", district);
        mstartupListDetailMap.put("osver", osver);
        mstartupListDetailMap.put("osbuilder", osbuilder);
        mstartupListDetailMap.put("devtype", devtype);
        _jedis.hmset(mstartupListValue, mstartupListDetailMap);


        //全局累计数据key
        String overallKey = "overalldata:" + game_abbr + ":" + system + ":" + platform_id + ":hash:incr";


        //1. 各系统所有启动设备列表
        String startupDevListKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":total:dev:set";
        _jedis.sadd(startupDevListKey, devid);

        Long overalldata_launchdev;
        Long overalldata_devjbs = 0L;
        Long specNum = _jedis.scard(startupDevListKey);

        if ("iosjb".equals(system)) {
            overalldata_devjbs = specNum;
            overalldata_launchdev = specNum + (!_jedis.exists("mstartup:" + game_abbr + ":ios:" + platform_id + ":total:dev:set") ?
                    0L : _jedis.scard("mstartup:" + game_abbr + ":ios:" + platform_id + ":total:dev:set"));
        } else {
            overalldata_launchdev = specNum;
        }

        _jedis.hset(overallKey, "launchdev", overalldata_launchdev.toString());
        _jedis.hset(overallKey, "devjbs", overalldata_devjbs.toString());


        //2. 各系统当天所有启动设备列表
        String startupDevListDailyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr +
                ":dev:set";
        _jedis.sadd(startupDevListDailyKey, devid);

        Long overalldatadaily_launchdev = _jedis.scard(startupDevListDailyKey);

        _jedis.hset(overallKey, "launchdev:"+todayStr, overalldatadaily_launchdev.toString());


        //3. 各系统当天各时段所有启动设备列表
        String startupDevListHourlyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayHourStr +
                ":dev:set";
        _jedis.sadd(startupDevListHourlyKey, devid);

        Long overalldatahourly_launchdev = _jedis.scard(startupDevListHourlyKey);

        _jedis.hset(overallKey, "launchdev:"+todayStr+":"+curHour, overalldatahourly_launchdev.toString());


        //4. 各系统当天各版本所有启动设备列表
        String startupDevListDailyVerlyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr +
                ":" + appver + ":dev:set";
        _jedis.sadd(startupDevListDailyVerlyKey, devid);

        Long overalldatadailyverly_launchdev = _jedis.scard(startupDevListDailyVerlyKey);

        _jedis.hset(overallKey, "launchdev:"+todayStr+":"+appver, overalldatadailyverly_launchdev.toString());

        //5. 各系统当天各区服各版本启动设备列表
        String startupDevListSvrDailyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":dev:set";
        _jedis.sadd(startupDevListSvrDailyKey, devid);

        Long signinlogindaily_launchdev = _jedis.scard(startupDevListSvrDailyKey);


        //6. 各系统当天各区服各版本设备启动次数
        String startupDevNumSvrDailyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":dev:incr";
        _jedis.incr(startupDevNumSvrDailyKey);

        Integer signinlogindaily_launch = Integer.parseInt(_jedis.get(startupDevNumSvrDailyKey));


        //7. 各系统当天各时段各区服各版本启动设备列表
        String startupDevListSvrHourlyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayHourStr + ":" + appver + ":dev:set";
        _jedis.sadd(startupDevListSvrHourlyKey, devid);

        Long signinloginhourly_launchdev = _jedis.scard(startupDevListSvrHourlyKey);


        //8. 各系统当天各区服各版本各时段设备启动次数
        String startupDevNumSvrHourlyKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayHourStr + ":" + appver + ":dev:incr";
        _jedis.incr(startupDevNumSvrHourlyKey);

        Integer signinloginhourly_launch = Integer.parseInt(_jedis.get(startupDevNumSvrHourlyKey));


        String startupDevListSvrKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":dev:set";
        //9. 各系统当天各区服各版本新增启动设备列表
        String startupDevListSvrNewKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":dev:new:set";
        //10. 各系统当天各时段各区服各版本新增启动设备列表
        String startupDevListSvrHourlyNewKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayHourStr + ":" + appver + ":dev:new:set";
        //11. 各系统当天各时段各分钟各区服各版本新增启动设备列表
        String startupDevListSvrMinlyNewKey = "mstartup:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayMinuteStr + ":" + appver + ":dev:new:set";
        if (1L == _jedis.sadd(startupDevListSvrKey, devid)) {
            _jedis.sadd(startupDevListSvrNewKey, devid);
            _jedis.sadd(startupDevListSvrHourlyNewKey, devid);
            _jedis.sadd(startupDevListSvrMinlyNewKey, devid);
        }

        Long signinlogindaily_newdev = !_jedis.exists(startupDevListSvrNewKey) ? 0L : _jedis.scard(startupDevListSvrNewKey);
        Long signinloginhourly_newdev = !_jedis.exists(startupDevListSvrHourlyNewKey) ? 0L : _jedis.scard(startupDevListSvrHourlyNewKey);
        Long newonlinert_newdev = !_jedis.exists(startupDevListSvrMinlyNewKey) ? 0L : _jedis.scard(startupDevListSvrMinlyNewKey);


        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;

            String inssql_overalldata = String.format("INSERT INTO overalldata (client, platform, launchdev, devjbs)" +
                    " VALUES (%d, %s, %d, %d) ON DUPLICATE KEY UPDATE launchdev=%d, devjbs=%d;", client, platform_id,
                    overalldata_launchdev, overalldata_devjbs, overalldata_launchdev, overalldata_devjbs);

            String inssql_overalldatadaily = String.format("INSERT INTO overalldatadaily (client, platform, date, launchdev)" +
                    " VALUES (%d, %s, %d, %d) ON DUPLICATE KEY UPDATE launchdev=%d;", client, platform_id, todayDate,
                    overalldatadaily_launchdev, overalldatadaily_launchdev);

            String overalldatahourly_tb = "overalldatahourly_" + todayStr;
            String dmlsql_overalldatahourly_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`hour` tiny(3) unsigned NOT NULL," +
                    "`hau` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`maxonline` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launchdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `hour`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", overalldatahourly_tb);
            String inssql_overalldatahourly = String.format("INSERT INTO %s (client, platform, hour, launchdev) VALUES " +
                    "(%d, %s, %s, %d) ON DUPLICATE KEY UPDATE launchdev=%d;", overalldatahourly_tb, client, platform_id,
                    curHour, overalldatahourly_launchdev, overalldatahourly_launchdev);

            String inssql_overalldatadailyverly = String.format("INSERT INTO overalldatadailyverly (client, platform, " +
                    "date, version, launchdev) VALUES (%d, %s, %d, %s, %d) ON DUPLICATE KEY UPDATE launchdev=%d;",
                    client, platform_id, todayDate, appver, overalldatadailyverly_launchdev, overalldatadailyverly_launchdev);

            String inssql_signinlogindaily = String.format("INSERT INTO signinlogindaily (client, platform, server, date," +
                    "version, newdev, launchdev, launch) VALUES (%d, %s, %s, %d, %s, %d, %d, %d) ON DUPLICATE KEY UPDATE" +
                    " newdev=%d, launchdev=%d, launch=%d;", client, platform_id, server_id, todayDate, appver,
                    signinlogindaily_newdev, signinlogindaily_launchdev, signinlogindaily_launch,
                    signinlogindaily_newdev, signinlogindaily_launchdev, signinlogindaily_launch);

            String signinloginhourly_tb = "signinloginhourly_" + todayStr;
            String dmlsql_signinloginhourly_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`hour` tiny(3) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`newdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`newacc` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`logins` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launchdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launch` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`lasttime` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `server`, `hour`, `version`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", signinloginhourly_tb);
            String inssql_signinloginhourly = String.format("INSERT INTO %s (client, platform, server, hour, version," +
                    "newdev, launchdev, launch) VALUES (%d, %s, %s, %s, %s, %d, %d, %d) ON DUPLICATE KEY UPDATE " +
                    "newdev=%d, launchdev=%d, launch=%d;", signinloginhourly_tb, client, platform_id, server_id, curHour,
                    appver, signinloginhourly_newdev, signinloginhourly_launchdev, signinloginhourly_launch,
                    signinloginhourly_newdev, signinloginhourly_launchdev, signinloginhourly_launch);

            String startuplist_tb = "startuplist_" + todayStr;
            String dmlsql_startuplist_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`account` char(128) CHARACTER SET UTF8 NOT NULL," +
                    "`devid` char(128) CHARACTER SET UTF8 NOT NULL," +
                    "PRIMARY KEY (`id`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", startuplist_tb);
            String inssql_startuplist = String.format("INSERT INTO %s (client, platform, server, version, account, " +
                    "devid) VALUES (%d, %s, %s, %s, %s, %s);", startuplist_tb, client, platform_id, server_id, appver,
                    "", devid);

            String newonlinert_tb = "newonlinert_" + todayStr;
            String dmlsql_newonlinert_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`datetime` int(11) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`online` int(10) unsigned NOT NULL DEFAULT 0," +
                    "`newdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`newacc` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `server`, `datetime`, `version`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", newonlinert_tb);
            String inssql_newonlinert = String.format("INSERT INTO %s (client, platform, server, datetime, version, newdev)" +
                    " VALUES (%d, %s, %s, %d, %s, %d) ON DUPLICATE KEY UPDATE newdev=%d;", newonlinert_tb, client, platform_id,
                    server_id, todayMinuteDate, appver, newonlinert_newdev, newonlinert_newdev);

//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldata);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldatadaily);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_overalldatahourly_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldatahourly);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldatadailyverly);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_signinlogindaily);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_signinloginhourly_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_signinloginhourly);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_startuplist_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_startuplist);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_newonlinert_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_newonlinert);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }

            String sql = inssql_overalldata;
            sql += inssql_overalldatadaily;
            sql += dmlsql_overalldatahourly_tb;
            sql += inssql_overalldatahourly;
            sql += inssql_overalldatadailyverly;
            sql += inssql_signinlogindaily;
            sql += dmlsql_signinloginhourly_tb;
            sql += inssql_signinloginhourly;
            sql += dmlsql_startuplist_tb;
            sql += inssql_startuplist;
            sql += dmlsql_newonlinert_tb;
            sql += inssql_newonlinert;

System.out.println(sql);
            try {
                sql_ret = _dbconnect.DirectUpdateBatch(game_abbr, sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }
System.out.println("|||||-----batch update result: " + sql_ret);


        }



        /*collector.emit(new Values(game_abbr, client, platform_id, server_id, devid, todayDate, todayStr, todayHourStr, curHour,
                overalldata_launchdev, overalldata_devjbs, overalldatadaily_launchdev, overalldatahourly_launchdev,
                overalldatadailyverly_launchdev, signinlogindaily_launchdev, signinlogindaily_launch, signinloginhourly_launchdev,
                signinloginhourly_launch));*/
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        /*declarer.declare(new Fields("game_abbr", "client", "platform_id", "server_id", "devid", "todayDate", "todayStr",
                "todayHourStr", "curHour", "overalldata_launchdev", "overalldata_devjbs", "overalldatadaily_launchdev",
                "overalldatahourly_launchdev", "overalldatadailyverly_launchdev", "signinlogindaily_launchdev",
                "signinlogindaily_launch", "signinloginhourly_launchdev", "signinloginhourly_launch"));*/
    }
}
