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
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/7/30.
 */
public class GAdthruGRegCRUDBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static mysql _dbconnect = null;

    private ArrayList _jobList = new ArrayList<String>() {{
        add(0, "");
        add(1, "mj_characters");
        add(2, "js_characters");
        add(3, "ds_characters");
        add(4, "no4_characters");
    }};

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
        int count1 = tuple.getInteger(5);
        int countip = tuple.getInteger(6);
        String hour = tuple.getString(7);
        int count2 = tuple.getInteger(8);
        String job_id = tuple.getString(9);
        int count3 = tuple.getInteger(10);
        String adplanning_id = tuple.getString(11);
        String chunion_subid = tuple.getString(12);
        int count5min = tuple.getInteger(13);
        Long tis_datetime_5min = tuple.getLong(14);
        int count1hour = tuple.getInteger(15);
        Long tis_datetime_1hour = tuple.getLong(16);
        String todayStr = tuple.getString(17);

        String jobRegDbCol = (String) _jobList.get(Integer.parseInt(job_id));
        String hourlyRegDbCol = "T"+hour;

        String ins_sql = String.format("INSERT INTO adplanning_signinLogin_today (adplanning_id, chunion_subid, platform, server, date, up_time, %s, daily_characters, %s, charip)" +
                " VALUES (%s, %s, %s, %s, %d, %d, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE up_time=%d, %s=%d, daily_characters=%d, %s=%d, charip=%d;",
                jobRegDbCol, hourlyRegDbCol, adplanning_id, chunion_subid, platform_id, server_id, todayDate, datetime, count3, count1, count2, countip, datetime, jobRegDbCol, count3, count1, hourlyRegDbCol, count2, countip);

        //写入ad_realtime表
        String ad_realtime_tbname = "ad_realtime_" + todayStr;
        String create_ad_realtime_sql = String.format("CREATE TABLE IF NOT EXISTS %s (`id` int(11) unsigned NOT NULL AUTO_INCREMENT,`adplanning_id` int(11) unsigned NOT NULL," +
                "`chunion_subid` int(11) unsigned NOT NULL DEFAULT 0,`datetime` int(11) unsigned NOT NULL,`duration_type` tinyint(3) unsigned NOT NULL," +
                "`p_reg` int(11) unsigned NOT NULL DEFAULT 0,`characters` int(11) unsigned NOT NULL DEFAULT 0,`ip` int(11) unsigned NOT NULL DEFAULT 0," +
                "`pv` int(11) unsigned NOT NULL DEFAULT 0,PRIMARY KEY (`id`),UNIQUE KEY (`adplanning_id`, `chunion_subid`, `datetime`, `duration_type`)) " +
                "ENGINE=MyISAM DEFAULT CHARSET=utf8;", ad_realtime_tbname);
        String ins_ad_realtime_5min_sql = String.format("INSERT INTO %s (adplanning_id, chunion_subid, datetime, duration_type, characters) VALUES (%s, %s, %d, 1, %d) " +
                "ON DUPLICATE KEY UPDATE characters=%d;", ad_realtime_tbname, adplanning_id, chunion_subid, tis_datetime_5min, count5min, count5min);
        String ins_ad_realtime_1hour_sql = String.format("INSERT INTO %s (adplanning_id, chunion_subid, datetime, duration_type, characters) VALUES (%s, %s, %d, 2, %d) " +
                "ON DUPLICATE KEY UPDATE characters=%d;", ad_realtime_tbname, adplanning_id, chunion_subid, tis_datetime_1hour, count1hour, count1hour);

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
System.out.println(create_ad_realtime_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, create_ad_realtime_sql);
System.out.println(sql_ret);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
System.out.println(ins_ad_realtime_5min_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_ad_realtime_5min_sql);
System.out.println(sql_ret);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
System.out.println(ins_ad_realtime_1hour_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_ad_realtime_1hour_sql);
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
