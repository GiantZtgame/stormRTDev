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
import redis.clients.jedis.Jedis;
import storm.qule_util.*;

import java.util.*;

public class AdRefBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

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
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //"game_abbr","platform","server","logtime","keywords","adplanning_id","chunion_subid","ip","uname"
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform = tuple.getStringByField("platform");
        String server = tuple.getStringByField("server");
        String logtime = tuple.getStringByField("logtime");
        String keywords = tuple.getStringByField("keywords");
        String adplanning_id = tuple.getStringByField("adplanning_id");
        String chunion_subid = tuple.getStringByField("chunion_subid");
        String ip = tuple.getStringByField("ip");

        //logtime 2013-11-25 08:34:48
        String todayStr = date.timestamp2str(date.str2timestamp(logtime), "yyyyMMdd");
        Long todayStamp = date.str2timestamp(todayStr);
        //adref
        String REF = platform + ":" + server + ":" + adplanning_id + ":" +chunion_subid;

        String r_adex_ip_td = "adex:" + REF + ":ip:" + todayStr + ":set";
        String r_adex_pv_td = "adex:" + REF + ":pv:" + todayStr + ":incr";
        String r_adloading_ip_td = "adloading:" + REF + ":ip:" + todayStr + ":set";
        String r_adloading_pv_td = "adloading:" + REF + ":pv:" + todayStr + ":incr";
        String r_adpost_ip_td = "adpost:" + REF + ":ip:" + todayStr + ":set";
        String r_adpost_pv_td = "adpost:" + REF + ":pv:" + todayStr + ":incr";
        String r_adclick_ip_td = "adclick:" + REF + ":ip:" + todayStr + ":set";
        String r_adclick_pv_td = "adclick:" + REF + ":pv:" + todayStr + ":incr";
        String r_adarrive_ip_td = "adarrive:" + REF + ":ip:" + todayStr + ":set";
        String r_adarrive_pv_td = "adarrive:" + REF + ":pv:" + todayStr + ":incr";

        //广告弹出
        //记录5分钟，1小时和当天广告弹出的ip和pv
        if (keywords.equals("adex")) {
            _jedis.sadd(r_adex_ip_td, ip);
            _jedis.incr(r_adex_pv_td);

            _jedis.expire(r_adex_ip_td, 24 * 60 * 60);
            _jedis.expire(r_adex_pv_td, 24 * 60 * 60);
        }
        //广告加载
        //记录当天广告加载的ip和pv
        if (keywords.equals("adloading")) {
            _jedis.sadd(r_adloading_ip_td, ip);
            _jedis.incr(r_adloading_pv_td);

            _jedis.expire(r_adloading_ip_td, 24 * 60 * 60);
            _jedis.expire(r_adloading_pv_td, 24 * 60 * 60);

        }
        //广告完展
        //记录当天广告完展的ip和pv
        if (keywords.equals("adpost")) {
            _jedis.sadd(r_adpost_ip_td, ip);
            _jedis.incr(r_adpost_pv_td);

            _jedis.expire(r_adpost_ip_td, 24 * 60 * 60);
            _jedis.expire(r_adpost_pv_td, 24 * 60 * 60);
        }
        //广告点击
        //记录当天广告点击的ip和pv
        if (keywords.equals("adclick")) {
            _jedis.sadd(r_adclick_ip_td, ip);
            _jedis.incr(r_adclick_pv_td);

            _jedis.expire(r_adclick_ip_td, 24 * 60 * 60);
            _jedis.expire(r_adclick_pv_td, 24 * 60 * 60);
        }
        //页面到达
        //记录当天广告到达的ip和pv
        if (keywords.equals("adarrive")) {
            _jedis.sadd(r_adarrive_ip_td, ip);
            _jedis.incr(r_adarrive_pv_td);

            _jedis.expire(r_adarrive_pv_td, 24 * 60 * 60);
            _jedis.expire(r_adarrive_ip_td, 24 * 60 * 60);
        }

        String adex_pv = _jedis.exists(r_adex_pv_td) ? _jedis.get(r_adex_pv_td) : "0";
        Long adex_ip = _jedis.exists(r_adex_ip_td) ? _jedis.scard(r_adex_ip_td) : 0l;

        String adloading_pv = _jedis.exists(r_adloading_pv_td) ? _jedis.get(r_adloading_pv_td) : "0";
        Long adloading_ip = _jedis.exists(r_adloading_ip_td) ? _jedis.scard(r_adloading_ip_td) : 0l;

        String adpost_pv = _jedis.exists(r_adpost_pv_td) ? _jedis.get(r_adpost_pv_td) : "0";
        Long adpost_ip = _jedis.exists(r_adpost_ip_td) ? _jedis.scard(r_adpost_ip_td) : 0l;

        String adclick_pv = _jedis.exists(r_adclick_pv_td) ? _jedis.get(r_adclick_pv_td) : "0";
        Long adclick_ip = _jedis.exists(r_adclick_ip_td) ? _jedis.scard(r_adclick_ip_td) : 0l;

        String adarrive_pv = _jedis.exists(r_adarrive_pv_td) ? _jedis.get(r_adarrive_pv_td) : "0";
        Long adarrive_ip = _jedis.exists(r_adarrive_ip_td) ? _jedis.scard(r_adarrive_ip_td) : 0l;
        System.out.println("广告弹出pv：" + adex_pv + "    广告弹出ip：" + adex_ip);
        System.out.println("广告加载pv：" + adloading_pv + "    广告加载ip：" + adloading_ip);
        System.out.println("广告完展pv：" + adpost_pv + "    广告完展ip：" + adpost_ip);
        System.out.println("广告点击pv：" + adclick_pv + "    广告点击ip：" + adclick_ip);
        System.out.println("页面到达pv：" + adarrive_pv + "    页面到达ip：" + adarrive_ip);
        System.out.println("==========================================");

        //数据库60s更新一次
        Long nowtime = System.currentTimeMillis() / 1000;
        Long uptime = date.str2timestamp(todayStr+" 23:59:00");
        if (nowtime > uptime) {
            _jedis.del("timer:adrealtime:60s");
        }
        if (!_jedis.exists("timer:adrealtime:60s")) {
            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

            Long up_time = System.currentTimeMillis() / 1000;
            JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

            //===================================表platform_adref_today=======================================//

            String tbname = "platform_adref_today";
            Map<String, Object> insert = new HashMap<String, Object>();
            Map<String, Object> update = new HashMap<String, Object>();

            insert.put("adplanning_id", adplanning_id);
            insert.put("chunion_subid", chunion_subid);
            insert.put("platform", platform);
            insert.put("server", server);
            insert.put("date", todayStamp);
            insert.put("up_time", up_time);
            insert.put("ad_hit", adclick_pv);
            insert.put("ad_hit_ip", adclick_ip);
            insert.put("landing", adarrive_pv);
            insert.put("landing_ip", adarrive_ip);
            insert.put("pv_ex", adex_pv);
            insert.put("ip_ex", adex_ip);
            insert.put("pv_load", adloading_pv);
            insert.put("ip_load", adloading_ip);
            insert.put("pv_post", adpost_pv);
            insert.put("ip_post", adpost_ip);

            update.put("up_time", up_time);
            update.put("ad_hit", adclick_pv);
            update.put("ad_hit_ip", adclick_ip);
            update.put("landing", adarrive_pv);
            update.put("landing_ip", adloading_ip);
            update.put("pv_ex", adex_pv);
            update.put("ip_ex", adex_ip);
            update.put("pv_load", adloading_pv);
            update.put("ip_load", adloading_ip);
            update.put("pv_post", adpost_pv);
            update.put("ip_post", adpost_ip);

            Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
            data.put("insert", insert);
            data.put("update", update);

            String sql = con.setSql("replace", tbname, data);
            if (con.add(sql)) {
                System.out.println("******* Success ********");
                _jedis.setex("timer:adrealtime:60s", 60, "1");
            }

        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word"));}
}