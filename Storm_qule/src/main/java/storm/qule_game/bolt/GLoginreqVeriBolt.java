package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.qule_util.*;

import java.io.IOException;
import java.io.InputStream;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by WXF on 2014/7/21.
 */
public class GLoginreqVeriBolt extends BaseBasicBolt {
    private static final String LOG_SIGN = "loginreq";
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
System.out.println("+++++++ GLoginreq verify prepare!!!");

        if (stormConf.get("isOnline").toString().equals("true")) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

        /*InputStream game_cfg_in = getClass().getResourceAsStream(_gamecfg);
        try {
            _prop.load(game_cfg_in);
        } catch (IOException e) {
            e.printStackTrace();
        }*/
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String sentence = tuple.getString(0);

        String[] logs;
        logs = sentence.split("\\|");
System.out.println("++++++++++++++++++" + sentence);
        if (8 == logs.length) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime_str = logs[4];
            String log_type = logs[5];
            String loginreq_ip = logs[6];
            String loginreq_params_str = logs[7];

            String[] loginreq_params_split = loginreq_params_str.split("&");
            Map<String, String> queryStringMap = new HashMap<String, String>(loginreq_params_split.length);
            String [] loginreq_params;
            for (String qs : loginreq_params_split) {
                loginreq_params = qs.split("=");
                String param_val = "";
                try {
                    param_val = loginreq_params[1];
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                queryStringMap.put(loginreq_params[0], param_val);
            }

            String uname = queryStringMap.get("userName");
            String idu = queryStringMap.get("idu");
            String ida = queryStringMap.get("ida");
            String ad = queryStringMap.get("ad");

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

                collector.emit( new Values(game_abbr, platform_id, server_id, datetime, loginreq_ip, loginreq_params_str, uname, idu, ida, ad));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "loginreq_datetime", "loginreq_ip", "loginreq_params", "uname", "idu", "ida", "ad"));
    }
}
