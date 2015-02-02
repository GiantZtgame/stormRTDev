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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/12/2.
 */
public class MLevelCalcBolt extends BaseBasicBolt {
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
        String level_datetime = tuple.getStringByField("level_datetime");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String level = tuple.getStringByField("level");
        Integer costTime = Integer.parseInt(tuple.getStringByField("costTime"));

        Long level_datetime_int = Long.parseLong(level_datetime);
        String todayStr = date.timestamp2str(level_datetime_int, "yyyyMMdd");

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

        Integer level_int = Integer.parseInt(level);

        //根据用户登录列表信息获取用户系统、版本号信息
        String system = "";
        String appver = "";
//        String model = "";
//        String resolution = "";
//        String operator = "";
//        String network = "";
//        String clientip = "";
//        String district = "";
//        String osver = "";
//        String osbuilder = "";
//        String devtype = "";

        String mloginListKey = "mlogin:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        List mloginListLatest = _jedis.lrange(mloginListKey, -1, -1);       //获取最近登陆信息
        if (!mloginListLatest.isEmpty()) {
            String mloginLatestDetailKey = mloginListLatest.get(0).toString();
            List<String> mloginLatestDetail = _jedis.hmget(mloginLatestDetailKey, "os", "appver"/*, "devid", "model",
                "resolution", "operator", "network", "clientip", "district", "osver", "osbuilder", "devtype"*/);
            if (!mloginLatestDetail.isEmpty()) {
                system = mloginLatestDetail.get(0);
                appver = mloginLatestDetail.get(1);
                //devid = mloginLatestDetail.get(2);
//                model = mloginLatestDetail.get(3);
//                resolution = mloginLatestDetail.get(4);
//                operator = mloginLatestDetail.get(5);
//                network = mloginLatestDetail.get(6);
//                clientip = mloginLatestDetail.get(7);
//                district = mloginLatestDetail.get(8);
//                osver = mloginLatestDetail.get(9);
//                osbuilder = mloginLatestDetail.get(10);
//                devtype = mloginLatestDetail.get(11);
            }
        }

        String mlevelTotalTimeKey = "mlevel:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":" + level + ":time:incr";
        String mlevelTotalTimesKey = "mlevel:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":" + level + ":times:incr";

        String mlevelSegmentAccFormerKey = "mlevel:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":" + level + ":";
        String mlevelSegmentAccLatterKey = ":acc:incr";

        String mlevelDistKey = "mlevel:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":" + level + ":lvdist:incr";

        Long mlevelTotalTime = _jedis.incrBy(mlevelTotalTimeKey, costTime);
        _jedis.expire(mlevelTotalTimeKey, 60 * 24 * 60 * 60);
        Long mlevelTotalTimes = _jedis.incr(mlevelTotalTimesKey);
        _jedis.expire(mlevelTotalTimesKey, 60 * 24 * 60 * 60);

        Integer client = system2client.turnSystem2ClientId(system);

        Integer mlevelSegmentId = 0;
        if (costTime <= 60) {
            mlevelSegmentId = 1;
        } else if (costTime > 60 && costTime <= 300) {
            mlevelSegmentId = 2;
        } else if (costTime > 300 && costTime <= 600) {
            mlevelSegmentId = 3;
        } else if (costTime > 600 && costTime <= 1800) {
            mlevelSegmentId = 4;
        } else if (costTime > 1800 && costTime <= 3600) {
            mlevelSegmentId = 5;
        } else if (costTime > 3600 && costTime <= 10800) {
            mlevelSegmentId = 6;
        } else if (costTime > 10800 && costTime <= 36000) {
            mlevelSegmentId = 7;
        } else if (costTime > 36000) {
            mlevelSegmentId = 8;
        }
        String mlevelSegmentAccKey = mlevelSegmentAccFormerKey + mlevelSegmentId.toString() + mlevelSegmentAccLatterKey;
        Long mlevelSegmentAcc = _jedis.incr(mlevelSegmentAccKey);
        _jedis.expire(mlevelSegmentAccKey, 60 * 24 * 60 * 60);



        //level distribution
        Long mlevelDistThisLvNum = _jedis.incr(mlevelDistKey);
        _jedis.expire(mlevelDistKey, 60 * 24 * 60 * 60);

        Integer formerLv = 0;
        Long mlevelDistFormerLvNum = 0L;
        if (level_int > 1) {
            formerLv = level_int - 1;
            String mlevelDistFormerLvKey = "mlevel:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + formerLv.toString() + ":lvdist:incr";
            mlevelDistFormerLvNum = !_jedis.exists(mlevelDistFormerLvKey) ? 0L :
                    Integer.parseInt(_jedis.get(mlevelDistFormerLvKey));
            if (mlevelDistFormerLvNum > 0L) {
                mlevelDistFormerLvNum = _jedis.incrBy(mlevelDistFormerLvKey, -1);
                _jedis.expire(mlevelDistFormerLvKey, 60 * 24 * 60 * 60);
            }
        }



        //flush to level cache list
        String mlevelFlushKey = "mlevel:" + game_abbr + ":" + todayDate + ":record";
        String mlevelFlushField = client.toString() + ":" + platform_id + ":" + server_id + ":" + appver + ":" + level;
        List<String> mlevelCachedDetail = new ArrayList<String>();
        mlevelCachedDetail = _jedis.hmget(mlevelFlushKey, mlevelFlushField);
        String mlevelFlushValue = "";
        if (mlevelCachedDetail.get(0) != null && !mlevelCachedDetail.isEmpty()) {
            mlevelFlushValue = mlevelCachedDetail.get(0).toString();
        }
        String[] mlevelFlushValueList = {};
        if (mlevelFlushValue != null && !mlevelFlushValue.equals("")) {
            mlevelFlushValueList = mlevelFlushValue.split(":");
        }
        String lv_time = mlevelTotalTime.toString(), lv_times = mlevelTotalTimes.toString(),
                lv_num = mlevelDistThisLvNum.toString(), seg1_acc = "0", seg2_acc = "0", seg3_acc = "0", seg4_acc = "0",
                seg5_acc = "0", seg6_acc = "0", seg7_acc = "0", seg8_acc = "0";
        try {
            seg1_acc = mlevelFlushValueList[3];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg2_acc = mlevelFlushValueList[4];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg3_acc = mlevelFlushValueList[5];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg4_acc = mlevelFlushValueList[6];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg5_acc = mlevelFlushValueList[7];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg6_acc = mlevelFlushValueList[8];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg7_acc = mlevelFlushValueList[9];
        } catch (ArrayIndexOutOfBoundsException e) {}
        try {
            seg8_acc = mlevelFlushValueList[10];
        } catch (ArrayIndexOutOfBoundsException e) {}

        switch (mlevelSegmentId) {
            case 1:
                seg1_acc = mlevelSegmentAcc.toString();
                break;
            case 2:
                seg2_acc = mlevelSegmentAcc.toString();
                break;
            case 3:
                seg3_acc = mlevelSegmentAcc.toString();
                break;
            case 4:
                seg4_acc = mlevelSegmentAcc.toString();
                break;
            case 5:
                seg5_acc = mlevelSegmentAcc.toString();
                break;
            case 6:
                seg6_acc = mlevelSegmentAcc.toString();
                break;
            case 7:
                seg7_acc = mlevelSegmentAcc.toString();
                break;
            case 8:
                seg8_acc = mlevelSegmentAcc.toString();
                break;
        }
        String mlevelFlushNewValue = lv_time + ":" + lv_times + ":" + lv_num + ":" + seg1_acc + ":" + seg2_acc + ":" +
                seg3_acc + ":" + seg4_acc + ":" + seg5_acc + ":" + seg6_acc + ":" + seg7_acc + ":" + seg8_acc;
        _jedis.hset(mlevelFlushKey, mlevelFlushField, mlevelFlushNewValue);




        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;
            String sqls = "";

            String dmlsql_level = "";
            String inssql_level = "";
            String upsql_level = "";
            String dmlsql_lvlist_tb = "";
            String inssql_lvlist = "";

            if (_dbFlushTimer.ifItsTime2FlushDb(client.toString()+platform_id+server_id+appver)) {
                String level_tb = "level_" + todayStr;
                dmlsql_level = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                        "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                        "`client` tinyint(3) unsigned NOT NULL," +
                        "`platform` mediumint(5) unsigned NOT NULL," +
                        "`server` mediumint(5) unsigned NOT NULL," +
                        "`date` int(11) unsigned NOT NULL," +
                        "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                        "`level` mediumint(5) unsigned NOT NULL DEFAULT 0," +
                        "`lv_time` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`lv_times` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`lv_num` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg1_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg2_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg3_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg4_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg5_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg6_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg7_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "`seg8_acc` int(11) unsigned NOT NULL DEFAULT 0," +
                        "PRIMARY KEY (`id`)," +
                        "UNIQUE KEY `platform` (`client`, `platform`, `server`, `date`, `version`, `level`)" +
                        ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", level_tb);
                String segment_col = "seg" + mlevelSegmentId.toString() + "_acc";
                inssql_level = String.format("INSERT INTO %s (client, platform, server, date, version, level, " +
                                "lv_time, lv_times, lv_num, %s) VALUES (%d, '%s', '%s', %d, '%s', '%s', %d, %d, %d, %d) ON DUPLICATE KEY " +
                                "UPDATE lv_time=%d, lv_times=%d, lv_num=%d, %s=%d;", level_tb, segment_col, client, platform_id,
                        server_id, todayDate, appver, level, mlevelTotalTime, mlevelTotalTimes, mlevelDistThisLvNum,
                        mlevelSegmentAcc, mlevelTotalTime, mlevelTotalTimes, mlevelDistThisLvNum, segment_col,
                        mlevelSegmentAcc);

                if (level_int > 1) {
                    upsql_level = String.format("UPDATE %s SET lv_num=%d WHERE client=%d, platform='%s', server='%s'," +
                                    "date=%d, version='%s', level='%s';", level_tb, mlevelDistFormerLvNum, client, platform_id, server_id,
                            todayDate, appver, formerLv);
                }
            }

            String lvlist_tb = "lvlist_" + todayStr;
            dmlsql_lvlist_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id`int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`account` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`cname` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`datetime` int(11) unsigned NOT NULL," +
                    "`level` mediumint(5) unsigned NOT NULL DEFAULT 0," +
                    "`lv_time` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", lvlist_tb);
            inssql_lvlist = String.format("INSERT INTO %s (platform, server, account, cname, datetime, level, lv_time" +
                            ") VALUES ('%s', '%s', '%s', '%s', %d, '%s', %d);", lvlist_tb, platform_id, server_id,
                    uname, cname, level_datetime_int, level, costTime);

            sqls += dmlsql_level;
            sqls += inssql_level;
            sqls += upsql_level;
            sqls += dmlsql_lvlist_tb;
            sqls += inssql_lvlist;

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