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
public class MStartupVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "mstartup";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("|||||+++++++ MStartup verify prepare!!!");
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

        //GMZQ|1|1|xxxxx|1401235412|mstartup|AABBCCDD|ios/iosjb/android|1.1|Samsungxxx|1920x1080|CMCC|3g|1.1.1.1|shanghai|ios7.1.2|Apple|iPhone 4S
        String sentence = tuple.getString(0);
        String[] logs;
        logs = sentence.split("\\|");
        if (18 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime = logs[4];
            String keywords = logs[5];
            String devid = logs[6];
            String system = logs[7];
            String appver = logs[8];
            String model = logs[9];
            String resolution = logs[10];
            String sp = logs[11];
            String network = logs[12];
            String client_ip = logs[13];
            String district = logs[14];
            String osver = logs[15];
            String osbuilder = logs[16];
            String devtype = logs[17];

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform_id + server_id + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
                token_gen = token_gen.substring(5, 10);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if (token_gen.equals(token) && keywords.equals(LOG_SIGN)) {
                collector.emit(new Values(game_abbr, platform_id, server_id, datetime, devid, system, appver, model,
                        resolution, sp, network, client_ip, district, osver, osbuilder, devtype));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "startup_datetime", "devid", "system",
                "appver", "model", "resolution", "sp", "network", "client_ip", "district", "osver", "osbuilder",
                "devtype"));
    }
}
