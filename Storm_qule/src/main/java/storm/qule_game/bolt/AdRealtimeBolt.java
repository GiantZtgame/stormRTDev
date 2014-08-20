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
import java.util.regex.Pattern;

public class AdRealtimeBolt extends BaseBasicBolt {
    private static Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;
    List<String> k = Arrays.asList("adreg", "adex", "adloading", "adpost", "adclick", "adarrive");

    /**
     * 加载配置文件
     * @param stormConf
     * @param context
     */
    public void prepare(Map stormConf, TopologyContext context) {
        if (Boolean.parseBoolean(stormConf.get("isOnline").toString())) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple input , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|2013-11-25 08:34:48|adloading|AHSG_100_100_1|0|127.0.0.1|http://www.baidu.com
        //注册
        //AHSG|100|1|2013-11-25 08:34:48|adreg|AHSG_100_100_1|0|testaccount|127.0.0.1|http://www.baidu.com
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");
        if (logs.length >= 8 ) {
            String keywords = logs[4];
            if (k.contains(keywords)) {
                String game_abbr = logs[0];
                String platform = logs[1];
                String server = logs[2];
                String datetime = logs[3];
                String ida = logs[5];
                String idu = logs[6];
                String ip = logs[7];

                String adplanning_id = "0";
                String chunion_subid = "0";
                //获取主线id AHSG_100_100_1
                String[] param = ida.split("_");
                if (param.length == 4) {
                    adplanning_id = param[3];
                    chunion_subid = idu;
                }
                Long datestamp = date.str2timestamp(datetime);
                String todayStr = date.timestamp2str(datestamp, "yyyyMMdd");

                //唯一键
                String PSG = platform + ":" + server + ":" + game_abbr;
                //redis key
                String r_adreg_5m = "adreg:" + PSG + ":5m:incr";
                String r_adreg_1h = "adreg:" + PSG + ":1h:incr";

                String r_adex_ip_5m = "adex:" + PSG + ":ip:5m:set";
                String r_adex_ip_1h = "adex:" + PSG + ":ip:1h:set";
                String r_adex_pv_5m = "adex:" + PSG + ":pv:5m:incr";
                String r_adex_pv_1h = "adex:" + PSG + ":pv:1h:incr";
                String r_adex_ip_td = "adex:" + PSG + ":ip:" + todayStr + ":set";
                String r_adex_pv_td = "adex:" + PSG + ":pv:" + todayStr + ":incr";

                String r_adloading_ip_td = "adloading:" + PSG + ":ip:" + todayStr + ":set";
                String r_adloading_pv_td = "adloading:" + PSG + ":pv:" + todayStr + ":incr";

                String r_adpost_ip_td = "adpost:" + PSG + ":ip:" + todayStr + ":set";
                String r_adpost_pv_td = "adpost:" + PSG + ":pv:" + todayStr + ":incr";

                String r_adclick_ip_td = "adclick:" + PSG + ":ip:" + todayStr + ":set";
                String r_adclick_pv_td = "adclick:" + PSG + ":pv:" + todayStr + ":incr";

                String r_adarrive_ip_td = "adarrive:" + PSG + ":ip:" + todayStr + ":set";
                String r_adarrive_pv_td = "adarrive:" + PSG + ":pv:" + todayStr + ":incr";

                //注册
                if (keywords.equals("adreg")) {
                    ip = logs[8];
                    _jedis.incr(r_adreg_5m);
                    _jedis.incr(r_adreg_1h);
                }
                //广告弹出
                if (keywords.equals("adex")) {
                    _jedis.sadd(r_adex_ip_5m, ip);
                    _jedis.sadd(r_adex_ip_1h, ip);
                    _jedis.incr(r_adex_pv_5m);
                    _jedis.incr(r_adex_pv_1h);

                    _jedis.incr(r_adex_pv_td);
                    _jedis.expire(r_adex_pv_td, 24 * 60 * 60);
                    if (!_jedis.sismember(r_adex_ip_td, ip)) {
                        _jedis.sadd(r_adex_ip_td, ip);
                        _jedis.expire(r_adex_ip_td, 24 * 60 * 60);
                    }
                }
                //广告加载
                if (keywords.equals("adloading")) {
                    _jedis.incr(r_adloading_pv_td);
                    _jedis.expire(r_adloading_pv_td, 24 * 60 * 60);
                    if (!_jedis.sismember(r_adloading_ip_td, ip)) {
                        _jedis.sadd(r_adloading_ip_td, ip);
                        _jedis.expire(r_adloading_ip_td, 24 * 60 * 60);
                    }

                }
                //广告完展
                if (keywords.equals("adpost")) {
                    _jedis.incr(r_adpost_pv_td);
                    _jedis.expire(r_adpost_pv_td, 24 * 60 * 60);
                    if (!_jedis.sismember(r_adpost_ip_td, ip)) {
                        _jedis.sadd(r_adpost_ip_td, ip);
                        _jedis.expire(r_adpost_ip_td, 24 * 60 * 60);
                    }

                }
                //广告点击
                if (keywords.equals("adclick")) {
                    _jedis.incr(r_adclick_pv_td);
                    _jedis.expire(r_adclick_pv_td, 24 * 60 * 60);
                    if (!_jedis.sismember(r_adclick_ip_td, ip)) {
                        _jedis.sadd(r_adclick_ip_td, ip);
                        _jedis.expire(r_adclick_ip_td, 24 * 60 * 60);
                    }
                }
                //页面到达
                if (keywords.equals("adarrive")) {
                    _jedis.incr(r_adarrive_pv_td);
                    _jedis.expire(r_adarrive_pv_td, 24 * 60 * 60);
                    if (!_jedis.sismember(r_adarrive_ip_td, ip)) {
                        _jedis.sadd(r_adarrive_ip_td, ip);
                        _jedis.expire(r_adarrive_ip_td, 24 * 60 * 60);
                    }
                }

                String adreg_5m = _jedis.exists(r_adreg_5m) ? _jedis.get(r_adreg_5m) : "0";
                String adreg_1h = _jedis.exists(r_adreg_1h) ? _jedis.get(r_adreg_1h) : "0";

                String adex_pv = _jedis.exists(r_adex_pv_td) ? _jedis.get(r_adex_pv_td) : "0";
                Long adex_ip = _jedis.exists(r_adex_ip_td) ? _jedis.scard(r_adex_ip_td) : 0l;

                Long adex_ip_5m = _jedis.exists(r_adex_ip_5m) ? _jedis.scard(r_adex_ip_5m) : 0l;
                Long adex_ip_1h = _jedis.exists(r_adex_ip_1h) ? _jedis.scard(r_adex_ip_1h) : 0l;

                String adex_pv_5m = _jedis.exists(r_adex_pv_5m) ? _jedis.get(r_adex_pv_5m) : "0";
                String adex_pv_1h = _jedis.exists(r_adex_pv_1h) ? _jedis.get(r_adex_pv_1h) : "0";

                String adloading_pv = _jedis.exists(r_adloading_pv_td) ? _jedis.get(r_adloading_pv_td) : "0";
                Long adloading_ip = _jedis.exists(r_adloading_ip_td) ? _jedis.scard(r_adloading_ip_td) : 0l;

                String adpost_pv = _jedis.exists(r_adpost_pv_td) ? _jedis.get(r_adpost_pv_td) : "0";
                Long adpost_ip = _jedis.exists(r_adpost_ip_td) ? _jedis.scard(r_adpost_ip_td) : 0l;

                String adclick_pv = _jedis.exists(r_adclick_pv_td) ? _jedis.get(r_adclick_pv_td) : "0";
                Long adclick_ip = _jedis.exists(r_adclick_ip_td) ? _jedis.scard(r_adclick_ip_td) : 0l;

                String adarrive_pv = _jedis.exists(r_adarrive_pv_td) ? _jedis.get(r_adarrive_pv_td) : "0";
                Long adarrive_ip = _jedis.exists(r_adarrive_ip_td) ? _jedis.scard(r_adarrive_ip_td) : 0l;

                System.out.println("======================================");
                System.out.println("广告注册5m：" + adreg_5m + " 广告注册1h：" + adreg_1h);
                System.out.println("广告弹出pv：" + adex_pv + " 广告弹出ip：" + adex_ip);
                System.out.println("广告加载pv：" + adloading_pv + " 广告加载ip：" + adloading_ip);
                System.out.println("广告完展pv：" + adpost_pv + " 广告完展ip：" + adpost_ip);
                System.out.println("广告点击pv：" + adclick_pv + " 广告点击ip：" + adclick_ip);
                System.out.println("页面到达pv：" + adarrive_pv + " 页面到达ip：" + adarrive_ip);
                System.out.println("======================================");

                //数据库60s更新一次
                if (!_jedis.exists("timer:adrealtime:60s")) {

                    Long up_time = System.currentTimeMillis() / 1000;
                    String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
                    String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
                    String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
                    String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
                    String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");

                    JdbcMysql con = JdbcMysql.getInstance(game_abbr,host, port, db, user, passwd);

                    //===================================表platform_adref_today=======================================//

                    String tbname = "platform_adref_today";
                    final Map<String, Object> insert = new HashMap<String, Object>();
                    final Map<String, Object> update = new HashMap<String, Object>();
                    insert.put("adplanning_id", adplanning_id);
                    insert.put("chunion_subid", chunion_subid);
                    insert.put("platform", platform);
                    insert.put("server", server);
                    insert.put("date", datestamp);
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

                    Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>() {{
                        put("insert", insert);
                        put("update", update);
                    }};
                    String sql = con.setSql("replace", tbname, data);

                    //===================================表 ad_realtime_* =======================================//

                    String tablename = "ad_realtime_" + todayStr;

                    String create_table = "CREATE TABLE IF NOT EXISTS `" + db + "`.`" + tablename + "`(" +
                            "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                            "`adplanning_id` int(11) unsigned NOT NULL COMMENT '主线ID'," +
                            "`chunion_subid` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '网盟子渠道ID，没有即为0'," +
                            "`datetime` int(11) unsigned NOT NULL COMMENT '时间点，默认5分钟间隔'," +
                            "`duration_type` tinyint(3) unsigned NOT NULL COMMENT '间隔类型：1:5分钟, 2:1小时'," +
                            "`p_reg` int(11) unsigned NOT NULL DEFAULT '0' COMMENT '该时间段内的平台注册'," +
                            "`characters` int(11) unsigned NOT NULL COMMENT '该时间段内的激活'," +
                            "`ip` int(11) unsigned NOT NULL COMMENT '该时间段内的独立IP'," +
                            "`pv` int(11) unsigned NOT NULL COMMENT '该时间段内的PV'," +
                            "        PRIMARY KEY (`id`)," +
                            "        UNIQUE KEY `adplanning_id` (`adplanning_id`,`chunion_subid`,`datetime`,`duration_type`)" +
                            ") ENGINE=InnoDB AUTO_INCREMENT=7 DEFAULT CHARSET=utf8 COMMENT='广告实时数据'";

                    Long tis_datetime_5m = up_time - up_time % 300;
                    Long tis_datetime_1h = up_time - up_time % 3600;
                    final Map<String, Object> insert_5m = new HashMap<String, Object>();
                    final Map<String, Object> insert_1h = new HashMap<String, Object>();
                    final Map<String, Object> update_5m = new HashMap<String, Object>();
                    final Map<String, Object> update_1h = new HashMap<String, Object>();

                    insert_5m.put("adplanning_id", adplanning_id);
                    insert_5m.put("chunion_subid", chunion_subid);
                    insert_5m.put("datetime", tis_datetime_5m);
                    insert_5m.put("duration_type", 1);
                    insert_5m.put("p_reg", adreg_5m);
                    insert_5m.put("characters", 0);
                    insert_5m.put("ip", adex_ip_5m);
                    insert_5m.put("pv", adex_pv_5m);

                    insert_1h.put("adplanning_id", adplanning_id);
                    insert_1h.put("chunion_subid", chunion_subid);
                    insert_1h.put("datetime", tis_datetime_1h);
                    insert_1h.put("duration_type", 2);
                    insert_1h.put("p_reg", adreg_1h);
                    insert_1h.put("characters", 0);
                    insert_1h.put("ip", adex_ip_1h);
                    insert_1h.put("pv", adex_pv_1h);

                    update_5m.put("p_reg", adreg_5m);
                    update_5m.put("ip", adex_ip_5m);
                    update_5m.put("pv", adex_pv_5m);

                    update_1h.put("p_reg", adreg_1h);
                    update_1h.put("ip", adex_ip_1h);
                    update_1h.put("pv", adex_pv_1h);

                    Map<String, Map<String, Object>> data_5m = new HashMap<String, Map<String, Object>>() {{
                        put("insert", insert_5m);
                        put("update", update_5m);
                    }};
                    Map<String, Map<String, Object>> data_1h = new HashMap<String, Map<String, Object>>() {{
                        put("insert", insert_1h);
                        put("update", update_1h);
                    }};
                    String sql_5m = con.setSql("replace", tablename, data_5m);
                    String sql_1h = con.setSql("replace", tablename, data_1h);

                    //=======================sql批量插入================================

                    List<String> sqls = new ArrayList();
                    sqls.add(create_table);
                    sqls.add(sql_5m);
                    sqls.add(sql_1h);
                    sqls.add(sql);

                    if (con.batchAdd(sqls)) {
                        //重新计时
                        if (!_jedis.exists("timer:adrealtime:5m")) {
                            _jedis.setex("timer:adrealtime:5m", 5 * 60, "1");
                            //删除所有key
                            _jedis.del(r_adreg_5m);
                            _jedis.del(r_adex_ip_5m);
                            _jedis.del(r_adex_pv_5m);
                        }
                        if (!_jedis.exists("timer:adrealtime:1h")) {
                            _jedis.setex("timer:adrealtime:1h", 60 * 60, "1");
                            //删除所有key
                            _jedis.del(r_adreg_1h);
                            _jedis.del(r_adex_ip_1h);
                            _jedis.del(r_adex_pv_1h);
                        }
                        System.out.println("******* Success ********");

                        //_jedis.setex("timer:adrealtime:60s", 60, "1");
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word"));}
}