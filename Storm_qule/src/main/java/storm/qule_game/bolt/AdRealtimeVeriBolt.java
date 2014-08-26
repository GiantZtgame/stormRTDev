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

import java.util.*;

public class AdRealtimeVeriBolt extends BaseBasicBolt {
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;
    List<String> K = Arrays.asList("adreg", "adex", "adloading", "adpost", "adclick", "adarrive");

    /**
     * 加载配置文件
     * @param stormConf
     * @param context
     */
    public void prepare(Map stormConf, TopologyContext context) {
        Boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
        if (isOnline) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
    }

    @Override
    public void execute(Tuple input , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|2013-11-25 08:34:48|adloading|AHSG_100_100_1|0|127.0.0.1|http://www.baidu.com
        //AHSG|100|1|2013-11-25 08:34:48|adreg|AHSG_100_100_1|0|testaccount|127.0.0.1|http://www.baidu.com
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length >= 8 ) {
            String keywords = logs[4];
            if (K.contains(keywords)) {
                String game_abbr = logs[0];
                String platform = logs[1];
                String server = logs[2];
                String logtime = logs[3];
                String ida = logs[5];
                String idu = logs[6];
                String ip = logs[7];
                String uname = "a";
                if (keywords.equals("adreg")) {
                    uname = logs[7];
                    ip = logs[8];
                }
                String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
                if (null != host) {
                    String adplanning_id = "0";
                    String chunion_subid = idu;
                    //获取主线id AHSG_100_100_1
                    String[] gpac = ida.split("_");
                    if (gpac.length == 4) {
                        adplanning_id = gpac[3];
                    }
                    collector.emit(new Values(game_abbr, platform, server, logtime,keywords,adplanning_id,chunion_subid,ip,uname));
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform","server","logtime","keywords","adplanning_id","chunion_subid","ip","uname"));
    }
}