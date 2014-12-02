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
public class MLoginVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "mlogin";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("|||||+++++++ MLogin verify prepare!!!");
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

        //GMZQ|1|1|26173|1401235412|mlogin|AABBCCDD|testaccount|1|ios/iosjb/android|1.1|Samsungxxx|1920x1080|CMCC|3g|1.1.1.1|shanghai|ios7.1.2|Apple|iPhone 4S
        String sentence = tuple.getString(0);
        String[] logs;
        logs = sentence.split("\\|");
        if (20 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime = logs[4];
            String keywords = logs[5];
            String devid = logs[6];
            String uname = logs[7];
            String login_success = logs[8];
            String system = logs[9];
            String appver = logs[10];
            String model = logs[11];
            String resolution = logs[12];
            String sp = logs[13];
            String network = logs[14];
            String client_ip = logs[15];
            String district = logs[16];
            String osver = logs[17];
            String osbuilder = logs[18];
            String devtype = logs[19];

            if (login_success.equals("1")) {
                String log_key = _prop.getProperty("game." + game_abbr + ".key");
                String raw_str = game_abbr + platform_id + server_id + log_key;
                String token_gen = "";
                try {
                    token_gen = new md5().gen_md5(raw_str);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if (token_gen.equals(token) && keywords.equals(LOG_SIGN)) {
                    collector.emit(new Values(game_abbr, platform_id, server_id, datetime, devid, uname, system, appver,
                            model, resolution, sp, network, client_ip, district, osver, osbuilder, devtype));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "login_datetime", "devid", "uname",
                "system", "appver", "model", "resolution", "sp", "network", "client_ip", "district", "osver",
                "osbuilder", "devtype"));
    }
}
