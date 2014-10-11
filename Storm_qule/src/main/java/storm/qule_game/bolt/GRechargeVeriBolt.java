package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.qule_util.*;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class GRechargeVeriBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

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
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
    }
    public void execute(Tuple input, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|131125-02:19:43|bill|1385212559|JW201311241109190353|100|qqq18986173900|战天无悔＿
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length == 11 || logs.length == 13) {
            String game_abbr = logs[0];
            String platform = logs[1];
            String server = logs[2];
            String token = logs[3];
            String keywords = logs[5];
            String logtime = logs[6];
            String order_id = logs[7];
            String amount = logs[8];
            String uname = logs[9];
            String cname = logs[10];
            String level = "-1";
            String jid = "-1";
            try {
                level = logs[11];
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }
            try {
                jid = logs[12];
            } catch (ArrayIndexOutOfBoundsException e) {
                e.printStackTrace();
            }

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            if (host != null) {
                if (keywords.equals("bill")) {
                    String log_key = _prop.getProperty("game." + game_abbr + ".key");
                    String raw_str = game_abbr + platform + server + log_key;
                    String token_gen = "";
                    try {
                        token_gen = new md5().gen_md5(raw_str);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    if (token_gen.equals(token)) {
                        collector.emit(new Values(game_abbr, platform, server, logtime,keywords,order_id,amount,uname,cname, level, jid));
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform","server","logtime","keywords","order_id","amount","uname","cname", "level", "jid"));
    }
}