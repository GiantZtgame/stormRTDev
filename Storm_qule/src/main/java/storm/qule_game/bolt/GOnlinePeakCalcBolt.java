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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/8/1.
 */
public class GOnlinePeakCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    Map<String, Integer>todayOnlinePeak = new HashMap<String, Integer>();

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

//        InputStream game_cfg_in = getClass().getResourceAsStream(_gamecfg);
//        try {
//            _prop.load(game_cfg_in);
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
        int online = Integer.parseInt(tuple.getString(4));
        int online_ip = Integer.parseInt(tuple.getString(5));

        String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

        String onlinePeakKey = "gonline:" + todayStr + ":" + game_abbr + ":" + platform_id + ":" + server_id;
        String maxKey = onlinePeakKey + ":max";
        String minKey = onlinePeakKey + ":min";
        String avgKey = onlinePeakKey + ":avg";
        String sumKey = onlinePeakKey + ":sum";
        String countKey = onlinePeakKey + ":count";
        String maxIpKey = onlinePeakKey + ":maxip";
        String minIpKey = onlinePeakKey + ":minip";
        String avgIpKey = onlinePeakKey + ":avgip";
        String sumIpKey = onlinePeakKey + ":sumip";
        String countIpKey = onlinePeakKey + ":countip";

        //最高在线人数
        //Integer maxOnlineChar = todayOnlinePeak.get(maxKey);
        Integer maxOnlineChar = !_jedis.exists(maxKey) ? 0 : Integer.parseInt(_jedis.get(maxKey));
        //if (null == maxOnlineChar) {
        //    maxOnlineChar = 0;
        //}
        if (online > maxOnlineChar) {
            maxOnlineChar = online;
            //todayOnlinePeak.put(maxKey, maxOnlineChar);
            _jedis.set(maxKey, maxOnlineChar.toString());
            _jedis.expire(maxKey, 24*60*60);
        }

        //最低在线人数
        //Integer minOnlineChar = todayOnlinePeak.get(minKey);
        Integer minOnlineChar = !_jedis.exists(minKey) ? 99999999 : Integer.parseInt(_jedis.get(minKey));
        //if (null == minOnlineChar) {
        //    minOnlineChar = 99999999;
        //}
        if (online < minOnlineChar) {
            minOnlineChar = online;
            //todayOnlinePeak.put(minKey, minOnlineChar);
            _jedis.set(minKey, minOnlineChar.toString());
            _jedis.expire(minKey, 24*60*60);
        }

        //在线人数和
        //Integer sumOnlineChar = todayOnlinePeak.get(sumKey);
        Integer sumOnlineChar = !_jedis.exists(sumKey) ? 0 : Integer.parseInt(_jedis.get(sumKey));
        //if (null == sumOnlineChar) {
        //    sumOnlineChar = 0;
        //}
        sumOnlineChar += online;
        //todayOnlinePeak.put(sumKey, sumOnlineChar);
        _jedis.set(sumKey, sumOnlineChar.toString());
        _jedis.expire(sumKey, 24*60*60);
        //在线人数次数
        //Integer countOnlineChar = todayOnlinePeak.get(countKey);
        Integer countOnlineChar = !_jedis.exists(countKey) ? 0 : Integer.parseInt(_jedis.get(countKey));
        //if (null == countOnlineChar) {
        //    countOnlineChar = 0;
        //}
        countOnlineChar++;
        //todayOnlinePeak.put(countKey, countOnlineChar);
        _jedis.set(countKey, countOnlineChar.toString());
        _jedis.expire(countKey, 24*60*60);
        //平均在线人数
        Integer avgOnlineChar = Math.round(sumOnlineChar/countOnlineChar);
        //todayOnlinePeak.put(avgKey, avgOnlineChar);
        _jedis.set(avgKey, avgOnlineChar.toString());
        _jedis.expire(avgKey, 24*60*60);

        //最高在线IP
        //Integer maxOnlineIp = todayOnlinePeak.get(maxIpKey);
        Integer maxOnlineIp = !_jedis.exists(maxIpKey) ? 0 : Integer.parseInt(_jedis.get(maxIpKey));
        //if (null == maxOnlineIp) {
        //    maxOnlineIp = 0;
        //}
        if (online_ip > maxOnlineIp) {
            maxOnlineIp = online_ip;
            //todayOnlinePeak.put(maxIpKey, maxOnlineIp);
            _jedis.set(maxIpKey, maxOnlineIp.toString());
            _jedis.expire(maxIpKey, 24*60*60);
        }

        //最低在线IP
        //Integer minOnlineIp = todayOnlinePeak.get(minIpKey);
        Integer minOnlineIp = !_jedis.exists(minIpKey) ? 99999999 : Integer.parseInt(_jedis.get(minIpKey));
        //if (null == minOnlineIp) {
        //    minOnlineIp = 99999999;
        //}
        if (online_ip < minOnlineIp) {
            minOnlineIp = online_ip;
            //todayOnlinePeak.put(minIpKey, minOnlineIp);
            _jedis.set(minIpKey, minOnlineIp.toString());
            _jedis.expire(minIpKey, 24*60*60);
        }

        //在线IP和
        //Integer sumOnlineIp = todayOnlinePeak.get(sumIpKey);
        Integer sumOnlineIp = !_jedis.exists(sumIpKey) ? 0 : Integer.parseInt(_jedis.get(sumIpKey));
        //if (null == sumOnlineIp) {
        //    sumOnlineIp = 0;
        //}
        sumOnlineIp += online_ip;
        //todayOnlinePeak.put(sumIpKey, sumOnlineIp);
        _jedis.set(sumIpKey, sumOnlineIp.toString());
        _jedis.expire(sumIpKey, 24*60*60);
        //在线IP次数
        //Integer countOnlineIp = todayOnlinePeak.get(countIpKey);
        Integer countOnlineIp = !_jedis.exists(countIpKey) ? 0 : Integer.parseInt(_jedis.get(countIpKey));
        //if (null == countOnlineIp) {
        //    countOnlineIp = 0;
        //}
        countOnlineIp++;
        //todayOnlinePeak.put(countIpKey, countOnlineIp);
        _jedis.set(countIpKey, countOnlineIp.toString());
        _jedis.expire(countIpKey, 24*60*60);
        //平均在线IP
        Integer avgOnlineIp = Math.round(sumOnlineIp/countOnlineIp);
        //todayOnlinePeak.put(avgIpKey, avgOnlineIp);
        _jedis.set(avgIpKey, avgOnlineIp.toString());
        _jedis.expire(avgIpKey, 24*60*60);



        collector.emit(new Values(game_abbr, platform_id, server_id, todayDate, datetime, maxOnlineChar, minOnlineChar, avgOnlineChar, maxOnlineIp, minOnlineIp, avgOnlineIp));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr", "platform_id", "server_id", "todayDate", "datetime", "maxOnlineChar", "minOnlineChar", "avgOnlineChar", "maxOnlineIp", "minOnlineIp", "avgOnlineIp"));
    }
}
