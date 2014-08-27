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
import java.util.*;

public class GTaskVeriBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;
    List<String> K = Arrays.asList("task_recv", "task_update", "task_fin","task_newbie");
    /**
     * 加载配置文件
     * @param stormConf
     * @param context
     */
    public void prepare(Map stormConf, TopologyContext context) {
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
        if (isOnline) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
    }
    @Override
    public void execute(Tuple input , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_recv|100101|zz|亂世逍遙|1
        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_update|100101|2|zz|亂世逍遙|3
        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_fin|100101|zz|亂世逍遙|5
        //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_newbie|zz|亂世逍遙|20
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length >= 10) {
            String game_abbr = logs[0];
            String platform = logs[1];
            String server = logs[2];
            String token = logs[3];
            String logtime = logs[4];
            String keywords = logs[5];

            String task_id = logs[6];
            String uname = logs[7];
            String cname = logs[8];
            String steps = "0";
            if (K.contains(keywords)) {

                if (keywords.equals("task_newbie")){
                    uname = logs[6];
                    cname = logs[7];
                }
                if (keywords.equals("task_update")) {
                    steps = logs[7];
                    uname = logs[8];
                    cname = logs[9];
                }
                String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
                if (null != host) {
                    //验证token
                    String log_key = _prop.getProperty("game." + game_abbr + ".key");
                    String raw_str = game_abbr + platform + server + log_key;
                    String token_gen = "";
                    try {
                        token_gen = new md5().gen_md5(raw_str);
                    } catch (NoSuchAlgorithmException e) {
                        e.printStackTrace();
                    }
                    if (token_gen.equals(token)) {
                        collector.emit(new Values(game_abbr, platform, server, logtime, keywords, task_id, uname, cname, steps));
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform","server","logtime","keywords","task_id","uname","cname","steps"));
    }
}