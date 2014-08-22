package storm.qule_game.bolt;
/**
 * Created by zhanghang on 2014/7/15.
 */

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;
import java.util.*;

public class AdRealtimeCalcBolt extends BaseBasicBolt {
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

        //"game_abbr","platform","server","todayStr","keywords","adplanning_id","chunion_subid","ip"
        String game_abbr = tuple.getStringByField("game_abbr");
        String todayStr = tuple.getStringByField("todayStr");
        String keywords = tuple.getStringByField("keywords");
        String adplanning_id = tuple.getStringByField("adplanning_id");
        String chunion_subid = tuple.getStringByField("chunion_subid");
        String ip = tuple.getStringByField("ip");

        Long nowtime = System.currentTimeMillis() / 1000;
        Long tis_datetime_5m = nowtime - nowtime % 300;
        Long tis_datetime_1h = nowtime - nowtime % 3600;
        //adrealtime
        String REAL = adplanning_id + ":" +chunion_subid;
        //redis key
        String r_adreg_5m = "adreg:" + REAL +":" + tis_datetime_5m+ ":5m:incr";
        String r_adreg_1h = "adreg:" + REAL +":" + tis_datetime_1h+ ":1h:incr";
        String r_adex_ip_5m = "adex:" + REAL +":" + tis_datetime_5m+ ":ip:5m:set";
        String r_adex_ip_1h = "adex:" + REAL +":" + tis_datetime_1h+ ":ip:1h:set";
        String r_adex_pv_5m = "adex:" + REAL +":" + tis_datetime_5m+ ":pv:5m:incr";
        String r_adex_pv_1h = "adex:" + REAL +":" + tis_datetime_1h+ ":pv:1h:incr";

        //注册
        //记录5分钟和1小时时间段注册人数
        if (keywords.equals("adreg")) {
            _jedis.incr(r_adreg_5m);
            _jedis.incr(r_adreg_1h);
            _jedis.expire(r_adreg_5m,5*60);
            _jedis.expire(r_adreg_1h,60*60);
        }
        //广告弹出
        //记录5分钟，1小时广告弹出的ip和pv
        if (keywords.equals("adex")) {
            _jedis.sadd(r_adex_ip_5m, ip);
            _jedis.sadd(r_adex_ip_1h, ip);

            _jedis.incr(r_adex_pv_5m);
            _jedis.incr(r_adex_pv_1h);

            _jedis.expire(r_adex_pv_5m,5*60);
            _jedis.expire(r_adex_pv_1h,60*60);
            _jedis.expire(r_adex_ip_5m,5*60);
            _jedis.expire(r_adex_ip_1h,60*60);
        }

        String adreg_5m = _jedis.exists(r_adreg_5m) ? _jedis.get(r_adreg_5m) : "0";
        String adreg_1h = _jedis.exists(r_adreg_1h) ? _jedis.get(r_adreg_1h) : "0";

        String adex_pv_5m = _jedis.exists(r_adex_pv_5m) ? _jedis.get(r_adex_pv_5m) : "0";
        String adex_pv_1h = _jedis.exists(r_adex_pv_1h) ? _jedis.get(r_adex_pv_1h) : "0";

        Long adex_ip_5m = _jedis.exists(r_adex_ip_5m) ? _jedis.scard(r_adex_ip_5m) : 0l;
        Long adex_ip_1h = _jedis.exists(r_adex_ip_1h) ? _jedis.scard(r_adex_ip_1h) : 0l;


        System.out.println("==============adrealtime================");
        System.out.println("5m广告注册数：" + adreg_5m);
        System.out.println("1h广告注册数：" + adreg_1h);
        System.out.println("5m广告弹出pv：" + adex_pv_5m + " 5m广告弹出ip：" + adex_ip_5m);
        System.out.println("1h广告弹出pv：" + adex_pv_1h + " 1h广告弹出ip：" + adex_ip_1h);
        System.out.println("======================================");

        //数据库60s更新一次
        if (!_jedis.exists("timer:adrealtime:60s")) {
            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");


            JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
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

            Map<String, Object> insert_5m = new HashMap<String, Object>();
            Map<String, Object> insert_1h = new HashMap<String, Object>();
            Map<String, Object> update_5m = new HashMap<String, Object>();
            Map<String, Object> update_1h = new HashMap<String, Object>();

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

            Map<String, Map<String, Object>> data_5m = new HashMap<String, Map<String, Object>>();
            data_5m.put("insert", insert_5m);
            data_5m.put("update", update_5m);

            Map<String, Map<String, Object>> data_1h = new HashMap<String, Map<String, Object>>();
            data_1h.put("insert", insert_1h);
            data_1h.put("update", update_1h);

            String sql_5m = con.setSql("replace", tablename, data_5m);
            String sql_1h = con.setSql("replace", tablename, data_1h);

            //=======================sql批量插入================================

            List<String> sqls = new ArrayList();
            sqls.add(create_table);
            sqls.add(sql_5m);
            sqls.add(sql_1h);
            if (con.batchAdd(sqls)) {
                System.out.println("******* Success ********");
                //_jedis.setex("timer:adrealtime:60s", 60, "1");
            }
        }

    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }
}