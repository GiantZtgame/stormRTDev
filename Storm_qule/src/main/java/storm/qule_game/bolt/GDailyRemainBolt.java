package storm.qule_game.bolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;

import java.util.*;

/**
 * Created by zhanghang on 2014/7/22.
 */
public class GDailyRemainBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        if (stormConf.get("isOnline").toString().equals("true")) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //[AHSG, 100, 1, 1385339688, 1, 59.51.56.166, mact2013, 119.97.171.104, 6020]
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        Long logtime = tuple.getLongByField("login_datetime");
        String uname = tuple.getStringByField("uname");

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        if (host != null) {

            String todayStr = date.timestamp2str(logtime, "yyyyMMdd");
            Long todayStamp = date.str2timestamp(todayStr);
            int someday[]={1,2,3,4,5,6,7,14,30};
            List<String> sqls = new ArrayList<String>();

            Map<String,Object> insert = new HashMap<String, Object>();
            Map<String,Object> update = new HashMap<String, Object>();
            insert.put("platform",platform_id);
            insert.put("server",server_id);
            insert.put("date",todayStamp);

            for (int day : someday) {
                Long somedayStamp = logtime - day * 24 * 60 * 60;
                String somedayStr = date.timestamp2str(somedayStamp, "yyyyMMdd");

                //===============================一般用户================================
                //唯一键
                String PSG = platform_id + ":" + server_id + ":" + game_abbr;
                String dailyremain = "gdailyremain:" + PSG + ":" + todayStr + ":" + day + ":set";
                String someday_char = "login:" + PSG + ":" + somedayStr + ":newchar:set";

                if (_jedis.sismember(someday_char, uname)) {
                    _jedis.sadd(dailyremain, uname);
                    //===============================广告用户================================
                    String hash_key = "gadinfo-" + platform_id + "-" + uname;
                    if (_jedis.exists(hash_key)) {

                        List<String> adinfo = _jedis.hmget(hash_key,"adplanning_id","chunion_subid");
                        String adplanning_id = adinfo.get(0);
                        String chunion_subid = adinfo.get(1);

                        if (Integer.parseInt(adplanning_id) > 0) {
                            String addailyremain = "addailyremain:"+ PSG + ":" + adplanning_id + ":" + chunion_subid + ":" + todayStr + ":" + day + ":set";
                            _jedis.sadd(addailyremain, uname);

                            Long adata = _jedis.scard(addailyremain);
                            String adsql = "INSERT INTO `adplanning_dailyRemain` (`adplanning_id`,`chunion_subid`,`platform`,`server`,`date`,`day" + day + "`) VALUES(" + adplanning_id + "," + chunion_subid + ","+platform_id + "," + server_id + "," + todayStamp + "," + adata +") ON DUPLICATE KEY UPDATE `day" + day + "` = " + adata+";";
                            sqls.add(adsql);
                        }
                    }
                }
                Long data = _jedis.scard(dailyremain);
                insert.put("day"+day,data);
                update.put("day"+day,data);
//                String sql = "INSERT INTO `opdata_dailyRemain` (`platform`,`server`,`date`,`day" + day + "`) VALUES(" + platform_id + "," + server_id + "," + todayStamp + "," + data +") ON DUPLICATE KEY UPDATE `day" + day + "` = " + data+";";
//                sqls.add(sql);
                System.out.println("第" + day + "日留存：" + data);
            }

            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
            JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

            Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
            data.put("insert", insert);
            data.put("update",update);
            String sql = con.setSql("replace", "opdata_dailyRemain", data);
            sqls.add(sql);
            con.batchAdd(sqls);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform_id","server_id","datetime","up_time","mj_characters","js_characters","ds_characters","daily_logins","daily_logins_char","daily_logins_new","daily_logins_char_new","daily_characters"));
    }
}
