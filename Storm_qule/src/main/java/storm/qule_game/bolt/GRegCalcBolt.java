package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;
import storm.qule_game.GRechargeTopology;
import storm.qule_util.cfgLoader;
import storm.qule_util.date;
import storm.qule_util.jedisUtil;
import storm.qule_util.timerCfgLoader;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * Created by wangxufeng on 2014/7/21.
 */
public class GRegCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

//    Map<String, Integer> todayRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayHourlyRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayJoblyRegCounts = new HashMap<String, Integer>();
//    Map<String, Integer> todayIpList = new HashMap<String, Integer>();
//    Map<String, Integer> todayIplyRegCounts = new HashMap<String, Integer>();

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
        String reg_success = tuple.getString(4);
        String uname = tuple.getString(5);
        String cname = tuple.getString(6);
        String job_id = tuple.getString(7);
        String client_ip = tuple.getString(8);

        if ("1".equals(reg_success)) {
            //当天总注册人数
            String todayStr = date.timestamp2str(datetime, "yyyyMMdd");

            Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");
            String key1 = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":incr";
            //Integer count1 = todayRegCounts.get(key1);
            Integer count1 = null == _jedis.get("gregCounts:" + key1) ? 0 : Integer.parseInt(_jedis.get("gregCounts:" + key1));
            //if (null == count1) {
            //    count1 = 0;
            //}
            count1++;
            //todayRegCounts.put(key1, count1);
            _jedis.set("gregCounts:" + key1, count1.toString());
            _jedis.expire("gregCounts:" + key1, 24*60*60);

            //当天总注册IP数
            String ipKey = todayStr + game_abbr + platform_id + server_id + client_ip;
            //Integer countip = todayIplyRegCounts.get(key1);
            Integer countip = null == _jedis.get("gregipCounts:" + key1) ? 0 : Integer.parseInt(_jedis.get("gregipCounts:" + key1));
            //if (null == countip) {
            //    countip = 0;
            //}
            //if (!todayIpList.containsKey(ipKey)) {
            //    todayIpList.put(ipKey, 1);
            //    countip++;
            //    todayIplyRegCounts.put(key1, countip);
            //}
            if (!_jedis.sismember("RegIpList:" + todayStr + ":set", ipKey)) {
                _jedis.sadd("RegIpList:" + todayStr + ":set", ipKey);
                _jedis.expire("RegIpList:" + todayStr + ":set", 30*24*60*60);
                countip++;
                _jedis.set("gregipCounts:" + key1, countip.toString());
                _jedis.expire("gregipCounts:" + key1, 24*60*60);

            }

            //当天按时段注册人数
            String todayHourStr = date.timestamp2str(datetime, "yyyyMMdd-HH");
            String hour = date.timestamp2str(datetime, "H");
            String key2 = todayHourStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":incr";
            //Integer count2 = todayHourlyRegCounts.get(key2);
            Integer count2 = null == _jedis.get("hourlyRegCounts:" + key2) ? 0 : Integer.parseInt(_jedis.get("hourlyRegCounts:" + key2));
            //if (null == count2) {
            //    count2 = 0;
            //}
            count2++;
            //todayHourlyRegCounts.put(key2, count2);
            _jedis.set("hourlyRegCounts:" + key2, count2.toString());
            _jedis.expire("hourlyRegCounts:" + key2, 24*60*60);

            //当天按职业注册人数
            String key3 = todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id + ":" + job_id + ":incr";
            //Integer count3 = todayJoblyRegCounts.get(key3);
            Integer count3 = null == _jedis.get("joblyRegCounts:" + key3) ? 0 : Integer.parseInt(_jedis.get("joblyRegCounts:" + key3));
            //if (null == count3) {
            //    count3 = 0;
            //}
            count3++;
            //todayJoblyRegCounts.put(key3, count3);
            _jedis.set("joblyRegCounts:" + key3, count3.toString());
            _jedis.expire("joblyRegCounts:" + key3, 24*60*60);

            //push to redis
            String listKey = "greginfo-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname;
            String listVal = "gregdetail-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname + "-" + String.valueOf(datetime);
            _jedis.rpush(listKey, listVal);

            String hashKey = listVal;
            Map<String, String> pairs = new HashMap<String, String>();
            pairs.put("regip", client_ip);
            pairs.put("cname", cname);
            pairs.put("job", job_id);
            _jedis.hmset(hashKey, pairs);

            //按职业登录数
            String PSG = platform_id + ":" + server_id + ":" + game_abbr;
            String joblyLoginJsonKey = "login:" + PSG + ":" + todayStr + ":joblychar:incr";
            String joblyLoginJson = null == _jedis.get(joblyLoginJsonKey) ? "[]" : _jedis.get(joblyLoginJsonKey);

            JSONArray joblyLoginJsonArray = new JSONArray(joblyLoginJson);
            JSONArray joblyLoginJsonArrayGen = new JSONArray();
            boolean jidExist = false;
            for(int i=0 ; i < joblyLoginJsonArray.length() ; i++) {
                String curJid = joblyLoginJsonArray.getJSONObject(i).getString("jid");
                Integer charCounts = joblyLoginJsonArray.getJSONObject(i).getInt("cc");
                if (curJid.equals(job_id)) {
                    charCounts++;
                    jidExist = true;
                }
                joblyLoginJsonArrayGen.put(new JSONObject().put("jid", curJid).put("cc", charCounts));
            }
            if (!jidExist) {
                joblyLoginJsonArrayGen.put(new JSONObject().put("jid", job_id).put("cc", 1));
            }
            joblyLoginJson = joblyLoginJsonArrayGen.toString();
            _jedis.set(joblyLoginJsonKey, joblyLoginJson);
            _jedis.expire(joblyLoginJsonKey, 24 * 60 * 60);


            collector.emit(new Values(game_abbr, platform_id, server_id, todayDate, datetime, count1, countip, hour, count2, job_id, count3, joblyLoginJson));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "todayDate", "datetime", "count1", "countip", "hour", "count2", "job_id", "count3", "joblyLoginJson"));
    }
}
