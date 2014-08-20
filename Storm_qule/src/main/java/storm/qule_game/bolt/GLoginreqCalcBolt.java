package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.cfgLoader;
import storm.qule_util.jedisUtil;
import storm.qule_util.timerCfgLoader;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/7/21.
 */
public class GLoginreqCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
System.out.println("&&&&&&&&&&&&&&&&&&GLoginreqCalcBolt prepare");
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());

        String conf;
        if (stormConf.get("isOnline").toString().equals("true")) {
            //conf = "/config/games.properties";
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

//        InputStream redis_cfg_in = getClass().getResourceAsStream(_gamecfg);
//        try {
//            _prop.load(redis_cfg_in);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String game_abbr = tuple.getString(0);
        String platform_id = tuple.getString(1);
        String server_id = tuple.getString(2);
        Long datetime = tuple.getLong(3);
        String ip = tuple.getString(4);
        String queryString = tuple.getString(5);
        String uname = tuple.getString(6);
        String idu = tuple.getString(7);
        String ida = tuple.getString(8);
        String ad = tuple.getString(9);

        //一般广告用户
        if ("1".equals(ad)) {
System.out.println("一般广告用户：" + uname + " " + ad);
            String hash_key = "gad-" + platform_id + "-" + uname;

            if (!_jedis.exists(hash_key)) {
System.out.println("一般广告用户：哈希key" + hash_key);
                Map<String, String> pairs = new HashMap<String, String>();
                pairs.put("ad", "1");
                _jedis.hmset(hash_key, pairs);
            }
        }

        //广告渠道用户
        if (null != ida && "" != ida) {
System.out.println("广告渠道用户：" + uname + " " + ida);
            String hash_key = "gadinfo-" + platform_id + "-" + uname;

            if (!_jedis.exists(hash_key)) {
                String[] ida_parts = ida.split("_");

                Map<String, String> pairs = new HashMap<String, String>();
                String chid_val = "", chposid_val = "", adplanning_id_val = "";
                try {
                    chid_val = ida_parts[1];
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                try {
                    chposid_val = ida_parts[2];
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                try {
                    adplanning_id_val = ida_parts[3];
                } catch (ArrayIndexOutOfBoundsException e) {
                    e.printStackTrace();
                }
                pairs.put("chid", chid_val);
                pairs.put("chposid", chposid_val);
                pairs.put("adplanning_id", adplanning_id_val);

                if (null != idu && "" != idu) {
                    pairs.put("chunion_subid", idu);
                } else {
                    pairs.put("chunion_subid", "0");
                }
                _jedis.hmset(hash_key, pairs);

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(""));
    }
}
