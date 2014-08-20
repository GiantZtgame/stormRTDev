package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import storm.qule_util.cfgLoader;
import storm.qule_util.date;
import storm.qule_util.jedisUtil;
import storm.qule_util.timerCfgLoader;

import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/7/30.
 */
public class GAdthruLoginCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

//    Map<String, Integer> todayAdthruLoginCharCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruLoginCharList = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruLoginIpCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruLoginIpList = new HashMap<String, Integer>();
//
//    Map<String, Integer> todayAdthruNewLoginCharCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruNewLoginCharList = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruNewLoginIpCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayAdthruNewLoginIpList = new HashMap<String, Integer>();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
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
//
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
        String login_success = tuple.getString(4);
        String login_ip = tuple.getString(5);
        String uname = tuple.getString(6);
        String gateway = tuple.getString(7);
        String port = tuple.getString(8);

        if (login_success.equals("1")) {
            //获取广告信息
            String hash_key = "gadinfo-" + platform_id + "-" + uname;
            if (_jedis.exists(hash_key)) {
                List<String> gadinfo = _jedis.hmget(hash_key, "chid", "chposid", "adplanning_id", "chunion_subid");
                String chid = gadinfo.get(0);
                String chposid = gadinfo.get(1);
                String adplanning_id = gadinfo.get(2);
                String chunion_subid = gadinfo.get(3);

                if (Integer.parseInt(adplanning_id) > 0) {
                    //当天总广告登录人数
                    String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
                    Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

                    String key1 = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + adplanning_id + ":" + chunion_subid + ":incr";

                    String charKey = todayStr + "-" + game_abbr + "-" + platform_id + "-" + server_id + "-" + adplanning_id + "-" + chunion_subid + "-" + uname;
                    //Integer countLoginChar  = todayAdthruLoginCharCounts.get(key1);
                    Integer countLoginChar = !_jedis.exists("adloginChar:" + key1) ? 0 : Integer.parseInt(_jedis.get("adloginChar:" + key1));
                    //当天广告新登
                    Integer countNewLoginChar = !_jedis.exists("adloginCharNew:" + key1) ? 0 : Integer.parseInt(_jedis.get("adloginCharNew:" + key1));
                    //if (null == countLoginChar) {
                    //    countLoginChar = 0;
                    //}
                    //if (!todayAdthruLoginCharList.containsKey(charKey)) {
                    //    todayAdthruLoginCharList.put(charKey, 1);
                    //    countLoginChar++;
                    //    todayAdthruLoginCharCounts.put(key1, countLoginChar);
                    //}
                    if (!_jedis.sismember("AdloginCharList:" + todayStr + ":set", charKey)) {
                        _jedis.sadd("AdloginCharList:" + todayStr + ":set", charKey);
                        _jedis.expire("AdloginCharList:" + todayStr + ":set", 30*24*60*60);
                        _jedis.incr("adloginChar:" + key1);
                        _jedis.expire("adloginChar:" + key1, 24*60*60);

                        countLoginChar = Integer.parseInt(_jedis.get("adloginChar:" + key1));

                        //是否新登
                        boolean ifNewLoginChar = false;
                        //if (!_jedis.sismember("totalChar_set", uname + platform_id + server_id + game_abbr)) {
                        //    ifNewLoginChar = true;
                        //}
                        if (!_jedis.sismember("login:"+platform_id+":"+server_id+":"+game_abbr+":total:char:set", uname)) {
                            ifNewLoginChar = true;
                        }

                        if (ifNewLoginChar) {
                            _jedis.incr("adloginCharNew:" + key1);
                            _jedis.expire("adloginCharNew:" + key1, 24*60*60);
                            countNewLoginChar = Integer.parseInt(_jedis.get("adloginCharNew:" + key1));
                        }

                    }

                    //当天总广告登录IP
                    String ipKey = todayStr + "-" + game_abbr + "-" + platform_id + "-" + server_id + "-" + adplanning_id + "-" + chunion_subid + "-" + login_ip;
                    //Integer countLoginIp = todayAdthruLoginIpCounts.get(key1);
                    Integer countLoginIp = !_jedis.exists("adloginIp:" + key1) ? 0 : Integer.parseInt(_jedis.get("adloginIp:" + key1));
                    //当天广告新登IP
                    Integer countNewLoginIp = !_jedis.exists("adloginIpNew:" + key1) ? 0 : Integer.parseInt(_jedis.get("adloginIpNew:" + key1));
                    //if (null == countLoginIp) {
                    //    countLoginIp = 0;
                    //}
                    //if (!todayAdthruLoginIpList.containsKey(ipKey)) {
                    //    todayAdthruLoginIpList.put(ipKey, 1);
                    //    countLoginIp++;
                    //    todayAdthruLoginIpCounts.put(key1, countLoginIp);
                    //}
                    if (!_jedis.sismember("AdloginIpList:" + todayStr + ":set", ipKey)) {
                        _jedis.sadd("AdloginIpList:" + todayStr + ":set", ipKey);
                        _jedis.expire("AdloginIpList:" + todayStr + ":set", 30*24*60*60);
                        _jedis.incr("adloginIp:" + key1);
                        _jedis.expire("adloginIp:" + key1, 24*60*60);

                        countLoginIp = Integer.parseInt(_jedis.get("adloginIp:" + key1));

                        //是否新登IP
                        boolean ifNewLoginIp = false;
                        //if (!_jedis.sismember("totalIp_set", uname + platform_id + server_id + game_abbr)) {
                        //    ifNewLoginIp = true;
                        //}
                        if (!_jedis.sismember("login:"+platform_id+":"+server_id+":"+game_abbr+":total:ip:set", login_ip)) {
                            ifNewLoginIp = true;
                        }

                        if (ifNewLoginIp) {
                            _jedis.incr("adloginIpNew:" + key1);
                            _jedis.expire("adloginIpNew:" + key1, 24*60*60);
                            countNewLoginIp = Integer.parseInt(_jedis.get("adloginIpNew:" + key1));
                        }
                    }

//                    //当天广告新登
//                    Integer countNewLoginChar = todayAdthruNewLoginCharCounts.get(key1);
//                    if (null == countNewLoginChar) {
//                        countNewLoginChar = 0;
//                    }
//                    if (!todayAdthruNewLoginCharList.containsKey(charKey)) {
//                        todayAdthruNewLoginCharList.put(charKey, 1);
//
//                        //是否新登
//                        boolean ifNewLoginChar = false;
////                        String charLoginLogKey = "glogininfo" + platform_id + game_abbr + server_id + uname;
////                        List<String> charEarliestLogin = _jedis.lrange(charLoginLogKey, 0, 0);
////                        //如果没有登陆过
////                        if (0 == charEarliestLogin.size()) {
////                            ifNewLoginChar = true;
////                        } else {
////                            for (int i = 0; i < charEarliestLogin.size(); i++) {
////                                String charEarlistLoginValStr = charEarliestLogin.get(i);
////                                String[] charEarlistLoginVal;
////                                charEarlistLoginVal = charEarlistLoginValStr.split("-");
////                                if (6 == charEarlistLoginVal.length) {
////                                    String earlistLoginTime = charEarlistLoginVal[5];
////                                    //如果首次登录为今日
////                                    if (Integer.parseInt(earlistLoginTime) >= todayDate) {
////                                        ifNewLoginChar = true;
////                                    }
////                                }
////                            }
////                        }
//                        if (!_jedis.sismember("totalChar_set", uname + platform_id + server_id + game_abbr)) {
//                            ifNewLoginChar = true;
//                        }
//
//                        if (ifNewLoginChar) {
//                            countNewLoginChar++;
//                            todayAdthruNewLoginCharCounts.put(key1, countNewLoginChar);
//                        }
//                    }

//                    //当天广告新登IP
//                    Integer countNewLoginIp = todayAdthruNewLoginIpCounts.get(key1);
//                    if (null == countNewLoginIp) {
//                        countNewLoginIp = 0;
//                    }
//                    if (!todayAdthruNewLoginIpList.containsKey(ipKey)) {
//                        todayAdthruNewLoginIpList.put(ipKey, 1);
//
//                        //是否新登IP
//                        boolean ifNewLoginIp = false;
//
//                        if (!_jedis.sismember("totalIp_set", uname + platform_id + server_id + game_abbr)) {
//                            ifNewLoginIp = true;
//                        }
//
//                        if (ifNewLoginIp) {
//                            countNewLoginIp++;
//                            todayAdthruNewLoginIpCounts.put(key1, countNewLoginIp);
//                        }
//                    }

                    collector.emit(new Values(game_abbr, platform_id, server_id, todayDate, datetime, countLoginChar, countLoginIp, countNewLoginChar, countNewLoginIp, adplanning_id, chunion_subid, uname, todayStr));
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "todayDate", "datetime", "countLoginChar", "countLoginIp", "countNewLoginChar", "countNewLoginIp", "adplanning_id", "chunion_subid", "uname", "todayStr"));
    }
}
