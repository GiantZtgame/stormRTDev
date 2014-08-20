package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.qule_util.cfgLoader;
import storm.qule_util.date;
import storm.qule_util.md5;
import storm.qule_util.timerCfgLoader;

import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

/**
 * Created by WXF on 2014/7/21.
 */
public class GRegVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "reg";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("+++++++ GReg verify prepare!!!");
        String conf;
        if (stormConf.get("isOnline").toString().equals("true")) {
            //conf = "/config/games.properties";
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

//        InputStream game_cfg_in = getClass().getResourceAsStream(_gamecfg);
//        try {
//            _prop.load(game_cfg_in);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String sentence = tuple.getString(0);

        String[] logs;
        logs = sentence.split("\\|");
        if (11 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime_str = logs[4];
            String log_type = logs[5];
            String reg_success = logs[6];
            String uname = logs[7];
            String cname = logs[8];
            String job_id = logs[9];
            String client_ip = logs[10];

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform_id + server_id + log_key;

            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if (token_gen.equals(token) && log_type.equals(LOG_SIGN)) {
                Long datetime = date.str2timestamp(datetime_str);

                collector.emit(new Values(game_abbr, platform_id, server_id, datetime, reg_success, uname, cname, job_id, client_ip));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "reg_datetime", "reg_success", "uname", "cname", "job_id", "client_ip"));
    }
}
