package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import storm.qule_util.cfgLoader;
import storm.qule_util.mysql;
import storm.qule_util.timerCfgLoader;

import java.io.IOException;
import java.io.InputStream;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/7/31.
 */
public class GAdthruLoginCRUDBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static mysql _dbconnect = null;

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (stormConf.get("isOnline").toString().equals("true")) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

        //InputStream game_cfg_in = getClass().getResourceAsStream("/config/games.properties");
//        InputStream game_cfg_in = getClass().getResourceAsStream(_gamecfg);
//        try {
//            _prop.load(game_cfg_in);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        _dbconnect = new mysql();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getString(0);
        String platform_id = tuple.getString(1);
        String server_id = tuple.getString(2);
        Long todayDate = tuple.getLong(3);
        Long datetime = tuple.getLong(4);
        int countLoginChar = tuple.getInteger(5);
        int countLoginIp = tuple.getInteger(6);
        int countNewLoginChar = tuple.getInteger(7);
        int countNewLoginIp = tuple.getInteger(8);
        String adplanning_id = tuple.getString(9);
        String chunion_subid = tuple.getString(10);
        String uname = tuple.getString(11);
        String todayStr = tuple.getString(12);

        String ins_sql = String.format("INSERT INTO adplanning_signinLogin_today (adplanning_id, chunion_subid, platform, server, date, up_time, daily_logins, daily_logins_char, daily_logins_new, daily_logins_char_new)" +
                        " VALUES (%s, %s, %s, %s, %d, %d, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE up_time=%d, daily_logins=%d, daily_logins_char=%d, daily_logins_new=%d, daily_logins_char_new=%d;",
                adplanning_id, chunion_subid, platform_id, server_id, todayDate, datetime, countLoginIp, countLoginChar, countNewLoginIp, countNewLoginChar, datetime, countLoginIp, countLoginChar, countNewLoginIp, countNewLoginChar);

        //写入广告用户登陆表(用于计算活跃用户)
        String adp_charlogin_tbname = "adp_charlogin_" + todayStr;
        String create_adp_charlogin_sql = String.format("CREATE TABLE IF NOT EXISTS %s  (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, " +
                "`platform` mediumint(5) unsigned NOT NULL, `server` mediumint(5) unsigned NOT NULL, `uname` char(128) CHARACTER SET utf8 NOT NULL, " +
                "`adplanning_id` int(11) unsigned NOT NULL, `chunion_subid` int(11) unsigned NOT NULL DEFAULT 0, PRIMARY KEY (`id`), " +
                "UNIQUE KEY (`platform`, `server`, `uname`)) ENGINE=MyISAM DEFAULT CHARSET=utf8;", adp_charlogin_tbname);
        String ins_adp_charlogin_sql = String.format("INSERT INTO %s (platform, server, uname, adplanning_id, chunion_subid) VALUES (%s, %s, '%s', %s, %s);",
                adp_charlogin_tbname, platform_id, server_id, uname, adplanning_id, chunion_subid);

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");


        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret;
            try {
System.out.println(ins_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_sql);
System.out.println(sql_ret);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
System.out.println(create_adp_charlogin_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, create_adp_charlogin_sql);
System.out.println(sql_ret);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
System.out.println(ins_adp_charlogin_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_adp_charlogin_sql);
System.out.println(sql_ret);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
            /*if (!_tc.push(game_abbr, ins_sql)) {
                _tc.pullAndFlush(game_abbr);
            }*/

System.out.println("@@@@@@ " + ins_sql);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        //declarer.declare(new Fields("word", "count"));
    }
}
