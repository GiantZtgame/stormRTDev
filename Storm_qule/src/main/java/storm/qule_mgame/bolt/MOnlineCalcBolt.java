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
 * Created by wangxufeng on 2014/11/25.
 */
public class MOnlineCalcBolt extends BaseBasicBolt {
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
        String online_datetime = tuple.getStringByField("online_datetime");
        String system = tuple.getStringByField("system");
        Integer online_acc = Integer.parseInt(tuple.getStringByField("online_acc"));
        Integer online_ip = Integer.parseInt(tuple.getStringByField("online_ip"));

        Long online_datetime_int = Long.parseLong(online_datetime);
        String todayStr = date.timestamp2str(online_datetime_int, "yyyyMMdd");
        String todayHourStr = date.timestamp2str(online_datetime_int, "yyyyMMdd-HH");
        String todayMinuteStr = date.timestamp2str(online_datetime_int, "yyyyMMdd-HHmm");
        String curHourMinute = date.timestamp2str(online_datetime_int, "HHmm");

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");
        Long todayMinuteDate = date.str2timestamp(todayMinuteStr, "yyyyMMdd-HHmm");

        Integer client = system2client.turnSystem2ClientId(system);

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
        String inssql_newonlinert = String.format("INSERT INTO %s (client, platform, server, datetime, version, online)" +
                " VALUES (%d, '%s', '%s', %d, '%s', %d) ON DUPLICATE KEY UPDATE online=%d;", newonlinert_tb, client,
                platform_id, server_id, online_datetime_int, "", online_acc, online_acc);


        String inssql_overalldatadaily = "";

        //================在线峰值================
        Integer MaxOnline = 0;
        Integer MinOnline = 0;
        Integer AvgOnline = 0;
        if (0 == client) {
            String onlinetotalKey = "monline:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":account:";
            String onlinetotalMaxKey = onlinetotalKey + "max";
            String onlinetotalMinKey = onlinetotalKey + "min";
            String onlinetotalAvgKey = onlinetotalKey + "avg";
            String onlinetotalSumKey = onlinetotalKey + "sum";
            String onlinetotalCountKey = onlinetotalKey + "count";

            //1. 总在线最高人数
            MaxOnline = !_jedis.exists(onlinetotalMaxKey) ? 0 : Integer.parseInt(_jedis.get(onlinetotalMaxKey));
            if (online_acc > MaxOnline) {
                MaxOnline = online_acc;
                _jedis.set(onlinetotalMaxKey, MaxOnline.toString());
                _jedis.expire(onlinetotalMaxKey, 24 * 60 * 60);
            }
            //2. 总在线最低人数
            MinOnline = !_jedis.exists(onlinetotalMinKey) ? 99999999 : Integer.parseInt(_jedis.get(onlinetotalMinKey));
            if (online_acc < MinOnline) {
                MinOnline = online_acc;
                _jedis.set(onlinetotalMinKey, MinOnline.toString());
                _jedis.expire(onlinetotalMinKey, 24 * 60 * 60);
            }
            //3. 总在线人数和
            Integer SumOnline = !_jedis.exists(onlinetotalSumKey) ? 0 : Integer.parseInt(_jedis.get(onlinetotalSumKey));
            SumOnline += online_acc;
            _jedis.set(onlinetotalSumKey, SumOnline.toString());
            _jedis.expire(onlinetotalSumKey, 24 * 60 * 60);
            //4. 总在线次数和
            Integer CountOnline = !_jedis.exists(onlinetotalCountKey) ? 0 : Integer.parseInt(_jedis.get(onlinetotalCountKey));
            CountOnline++;
            _jedis.set(onlinetotalCountKey, CountOnline.toString());
            _jedis.expire(onlinetotalCountKey, 24 * 60 * 60);
            //5. 总在线平均人数
            AvgOnline = Math.round(SumOnline / CountOnline);
            _jedis.set(onlinetotalAvgKey, AvgOnline.toString());
            _jedis.expire(onlinetotalAvgKey, 24 * 60 * 60);

        } else {
            String onlineKey = "monline:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" + todayStr +
                    ":" + "" + ":account:";
            String onlineMaxKey = onlineKey + "max";
            String onlineMinKey = onlineKey + "min";
            String onlineAvgKey = onlineKey + "avg";
            String onlineSumKey = onlineKey + "sum";
            String onlineCountKey = onlineKey + "count";

            //1. 各客户端各版本在线最高人数
            MaxOnline = !_jedis.exists(onlineMaxKey) ? 0 : Integer.parseInt(_jedis.get(onlineMaxKey));
            if (online_acc > MaxOnline) {
                MaxOnline = online_acc;
                _jedis.set(onlineMaxKey, MaxOnline.toString());
                _jedis.expire(onlineMaxKey, 24 * 60 * 60);
            }
            //2. 各客户端各版本在线最低人数
            MinOnline = !_jedis.exists(onlineMinKey) ? 99999999 : Integer.parseInt(_jedis.get(onlineMinKey));
            if (online_acc < MinOnline) {
                MinOnline = online_acc;
                _jedis.set(onlineMinKey, MinOnline.toString());
                _jedis.expire(onlineMinKey, 24 * 60 * 60);
            }
            //3. 各客户端各版本在线人数和
            Integer SumOnline = !_jedis.exists(onlineSumKey) ? 0 : Integer.parseInt(_jedis.get(onlineSumKey));
            SumOnline += online_acc;
            _jedis.set(onlineSumKey, SumOnline.toString());
            _jedis.expire(onlineSumKey, 24 * 60 * 60);
            //4. 各客户端各版本在线次数和
            Integer CountOnline = !_jedis.exists(onlineCountKey) ? 0 : Integer.parseInt(_jedis.get(onlineCountKey));
            CountOnline++;
            _jedis.set(onlineCountKey, CountOnline.toString());
            _jedis.expire(onlineCountKey, 24 * 60 * 60);
            //5. 各客户端各版本在线平均人数
            AvgOnline = Math.round(SumOnline / CountOnline);
            _jedis.set(onlineAvgKey, AvgOnline.toString());
            _jedis.expire(onlineAvgKey, 24 * 60 * 60);
        }


        //================在线趋势：最高/平均在线================
        Long overallMaxOnline;
        Integer overallAvgOnline;
        String overallOnlinetotalMaxKey = "monline:" + game_abbr + ":" + system + ":" + todayStr + ":account:max";
        String overallOnlinetotalAvgKey = "monline:" + game_abbr + ":" + system + ":" + todayStr + ":account:avg";
        String overallOnlinetotalSumKey = "monline:" + game_abbr + ":" + system + ":" + todayStr + ":account:sum";
        String overallOnlinetotalCountKey = "monline:" + game_abbr + ":" + system + ":" + todayStr + ":account:count";
        String overallOnlineMinuteKey = "monline:" + game_abbr + ":" + system + ":" + todayMinuteStr + ":account:incr";

        //记录本分钟的所有平台/区服在线数
        Long overallOnlineMinuteAccounts = !_jedis.exists(overallOnlineMinuteKey) ? 0L :
                Integer.parseInt(_jedis.get(overallOnlineMinuteKey));
        overallOnlineMinuteAccounts += online_acc;
        _jedis.set(overallOnlineMinuteKey, overallOnlineMinuteAccounts.toString());
        _jedis.expire(overallOnlineMinuteKey, 24 * 60 * 60);

        //获取上一分钟所有平台/区服在线数
        Long todayLastMinuteDate = "0000".equals(curHourMinute) ? todayMinuteDate : todayMinuteDate - 60;
        String todayLastMinuteStr = date.timestamp2str(todayLastMinuteDate, "yyyyMMdd-HHmm");
        String overallOnlineLastMinuteKey = "monline:" + game_abbr + ":" + system + ":" + todayLastMinuteStr +
                ":account:incr";
        Long overallOnlineLastMinuteAccounts = !_jedis.exists(overallOnlineLastMinuteKey) ? 0L :
                Integer.parseInt(_jedis.get(overallOnlineLastMinuteKey));

        //当天已记录的最高在线
        overallMaxOnline = !_jedis.exists(overallOnlinetotalMaxKey) ? 0L :
                Integer.parseInt(_jedis.get(overallOnlinetotalMaxKey));

        //比较
        overallMaxOnline = (overallMaxOnline >= overallOnlineLastMinuteAccounts ?
                overallMaxOnline : overallOnlineLastMinuteAccounts);
        _jedis.set(overallOnlinetotalMaxKey, overallMaxOnline.toString());
        _jedis.expire(overallOnlinetotalMaxKey, 24 * 60 * 60);

        //Long overallSumOnline = !_jedis.exists(overallOnlinetotalSumKey) ? 0L :
        //        Integer.parseInt(_jedis.get(overallOnlinetotalSumKey));
        //overallSumOnline += online_acc;
        Long overallSumOnline = _jedis.incrBy(overallOnlinetotalSumKey, online_acc);
        _jedis.expire(overallOnlinetotalSumKey, 24 * 60 * 60);
        //Long overallCountOnline = !_jedis.exists(overallOnlinetotalCountKey) ? 0L :
        //        Integer.parseInt(_jedis.get(overallOnlinetotalCountKey));
        //overallCountOnline++;
        Long overallCountOnline = _jedis.incr(overallOnlinetotalCountKey);
        _jedis.expire(overallOnlinetotalCountKey, 24 * 60 * 60);

        overallAvgOnline = Math.round(overallSumOnline / overallCountOnline);
        _jedis.set(overallOnlinetotalAvgKey, overallAvgOnline.toString());
        _jedis.expire(overallOnlinetotalAvgKey, 24 * 60 * 60);

        inssql_overalldatadaily = String.format("INSERT INTO overalldatadaily (client, platform, date, maxonline," +
                        "avgonline) VALUES (%d, '%s', %d, %d, %d) ON DUPLICATE KEY UPDATE maxonline=%d, avgonline=%d;",
                client, platform_id, todayDate, overallMaxOnline, overallAvgOnline, overallMaxOnline,
                overallAvgOnline);




        String inssql_onlineht = String.format("INSERT INTO onlineht (client, platform, server, date, version, max, min," +
                "avg) VALUES (%d, '%s', '%s', %d, '%s', %d, %d, %d) ON DUPLICATE KEY UPDATE max=%d, min=%d, avg=%d;",
                client, platform_id, server_id, todayDate, "", MaxOnline, MinOnline, AvgOnline, MaxOnline, MinOnline,
                AvgOnline);

        String sqls = dmlsql_newonlinert_tb;
        sqls += inssql_newonlinert;
        sqls += inssql_overalldatadaily;
        sqls += inssql_onlineht;

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)
                && _dbFlushTimer.ifItsTime2FlushDb(client.toString()+platform_id+server_id)) {
            boolean sql_ret = false;
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