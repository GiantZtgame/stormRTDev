package storm.qule_mgame.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.qule_util.cfgLoader;
import storm.qule_util.md5;
import storm.qule_util.timerCfgLoader;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/11/24.
 */
public class MBillVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "mbill";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("|||||+++++++ MBill verify prepare!!!");
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
        if (isOnline) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //GMZQ|1|1|xxxxx|1401235412|mbill|1401235415|order_12345|12|120|testaccount|津巴布韦国家队|1|66
        String sentence = tuple.getString(0);
        String[] logs;
        logs = sentence.split("\\|");
        if (14 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime = logs[4];
            String keywords = logs[5];
            String rechtime = logs[6];
            String order_id = logs[7];
            String amount = logs[8];
            String isk = logs[9];
            String uname = logs[10];
            String cname = logs[11];
            String ifsuccess = logs[12];
            String level = logs[13];

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform_id + server_id + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if (token_gen.equals(token) && keywords.equals(LOG_SIGN)) {
                collector.emit(new Values(game_abbr, platform_id, server_id, datetime, rechtime, order_id, amount, isk,
                        uname, cname, ifsuccess, level));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "bill_datetime", "rechtime", "order_id",
                "amount", "isk", "uname", "cname", "ifsuccess", "level"));
    }
}
