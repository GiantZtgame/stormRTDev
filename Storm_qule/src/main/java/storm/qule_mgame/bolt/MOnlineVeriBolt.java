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
 * Created by wangxufeng on 2014/11/22.
 */
public class MOnlineVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "monline";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("|||||+++++++ MOnline verify prepare!!!");
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

        //GMZQ|1|1|xxxxx|1401235412|monline|total|88888|20000
        //GMZQ|1|1|xxxxx|1401235412|monline|ios|11111|11111
        //GMZQ|1|1|xxxxx|1401235412|monline|android|77777|11111
        String sentence = tuple.getString(0);
        String[] logs;
        logs = sentence.split("\\|");
        if (9 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime = logs[4];
            String keywords = logs[5];
            String system = logs[6];
            String online_acc = logs[7];
            String online_ip = logs[8];

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform_id + server_id + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
            if (token_gen.equals(token) && keywords.equals(LOG_SIGN)) {
                collector.emit(new Values(game_abbr, platform_id, server_id, datetime, system, online_acc, online_ip));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "online_datetime", "system", "online_acc",
                "online_ip"));
    }
}