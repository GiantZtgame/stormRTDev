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
 * Created by wangxufeng on 2014/8/1.
 */
public class GOnlinePeakCRUDBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static mysql _dbconnect = null;

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
System.out.println("@@@@@@ CRUDOnlineData prepare!!");
        if (stormConf.get("isOnline").toString().equals("true")) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

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
        int maxOnlineChar = tuple.getInteger(5);
        int minOnlineChar = tuple.getInteger(6);
        int avgOnlineChar = tuple.getInteger(7);
        int maxOnlineIp = tuple.getInteger(8);
        int minOnlineIp = tuple.getInteger(9);
        int avgOnlineIp = tuple.getInteger(10);

        String ins_sql = String.format("INSERT INTO opdata_onlineHt VALUES (null, %s, %s, %d, %d, %d, %d, %d, %d, %d)" +
                " ON DUPLICATE KEY UPDATE max=%d, min=%d, avg=%d, max_ip=%d, min_ip=%d, avg_ip=%d;"
                , platform_id, server_id, todayDate, maxOnlineChar, minOnlineChar, avgOnlineChar, maxOnlineIp, minOnlineIp, avgOnlineIp
                , maxOnlineChar, minOnlineChar, avgOnlineChar, maxOnlineIp, minOnlineIp, avgOnlineIp);

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");
        System.out.println("@@@@@@ " + mysql_host + " " + mysql_port + " " + mysql_db + " " + mysql_user + " " + mysql_passwd);
        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret;
            try {
                System.out.println(ins_sql);
                sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_sql);
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
