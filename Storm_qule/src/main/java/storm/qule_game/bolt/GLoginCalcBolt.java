package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import org.json.JSONArray;
import org.json.JSONObject;
import redis.clients.jedis.Jedis;

import storm.qule_util.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhanghang on 2014/7/22.
 */
public class GLoginCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
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
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);
        if (tuple.size() == 9) {
            //"game_abbr", "platform_id", "server_id", "login_datetime", "login_success", "login_ip", "uname", "login_gateway", "login_port"
            String game_abbr = tuple.getStringByField("game_abbr");
            String platform_id = tuple.getStringByField("platform_id");
            String server_id = tuple.getStringByField("server_id");
            Long log_datetime = tuple.getLongByField("login_datetime");
            String todayStr = date.timestamp2str(log_datetime, "yyyyMMdd");
            String ip = tuple.getStringByField("login_ip");
            String uname = tuple.getStringByField("uname");

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
            if (host != null) {
                String adkey = "gadinfo-" + platform_id + "-" + uname;
                String chid = "0";
                String chposid = "0";
                String adplanning_id = "0";
                String chunion_subid = "0";
                if (_jedis.exists(adkey)) {
                    List<String> gadinfo = _jedis.hmget(adkey, "chid", "chposid", "adplanning_id", "chunion_subid");
                    chid = gadinfo.get(0);
                    chposid = gadinfo.get(1);
                    adplanning_id = gadinfo.get(2);
                    chunion_subid = gadinfo.get(3);
                }
                String PSG = platform_id + ":" + server_id + ":" + game_abbr;
                //redis key
                String tt_ip = "login:" + PSG + ":total:ip:set";
                String tt_char = "login:" + PSG + ":total:char:set";

                String td_ip = "login:" + PSG + ":" + todayStr + ":ip:set";
                String td_char = "login:" + PSG + ":" + todayStr + ":char:set";

                String new_ip = "login:" + PSG + ":" + todayStr + ":newip:set";
                String new_char = "login:" + PSG + ":" + todayStr + ":newchar:set";

                String todayHourStr = date.timestamp2str(log_datetime, "yyyyMMdd-HH");
                String curHour = date.timestamp2str(log_datetime, "H");
                String hourlyLoginCountsKey = "login:" + PSG + ":" + todayHourStr + ":hourlychar:incr";
                String hourlyNewLoginCountsKey = "login:" + PSG + ":" + todayHourStr + ":hourlynewchar:incr";
                boolean hourlyLoginFlag = false;

                String joblyLoginJsonKey = "login:" + PSG + ":" + todayStr + ":joblychar:incr";
                boolean joblyLoginFlag = false;

                //按时段登录数 按职业登录数
                Integer hourlyLoginCounts = null == _jedis.get(hourlyLoginCountsKey) ? 0 : Integer.parseInt(_jedis.get(hourlyLoginCountsKey));
                String joblyLoginJson = null == _jedis.get(joblyLoginJsonKey) ? "[]" : _jedis.get(joblyLoginJsonKey);
                if (!_jedis.sismember(td_char, uname)) {
                    hourlyLoginCounts++;
                    _jedis.set(hourlyLoginCountsKey, hourlyLoginCounts.toString());
                    _jedis.expire(hourlyLoginCountsKey, 24 * 60 * 60);
                    hourlyLoginFlag = true;

                    //获取用户角色信息
                    String regkey = "greginfo-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname;
                    if (_jedis.exists(regkey)) {
                        List userinfo = _jedis.lrange(regkey, 0, 0);
                        String reginfoHashKey = userinfo.get(0).toString();
                        List<String> reginfo = _jedis.hmget(reginfoHashKey, "job");
                        String jid = reginfo.get(0);

                        JSONArray joblyLoginJsonArray = new JSONArray(joblyLoginJson);
                        JSONArray joblyLoginJsonArrayGen = new JSONArray();
                        boolean jidExist = false;
                        for(int i=0 ; i < joblyLoginJsonArray.length() ; i++) {
                            String curJid = joblyLoginJsonArray.getJSONObject(i).getString("jid");
                            Integer charCounts = joblyLoginJsonArray.getJSONObject(i).getInt("cc");
                            if (curJid.equals(jid)) {
                                charCounts++;
                                jidExist = true;
                            }
                            joblyLoginJsonArrayGen.put(new JSONObject().put("jid", curJid).put("cc", charCounts));
                        }
                        if (!jidExist) {
                            joblyLoginJsonArrayGen.put(new JSONObject().put("jid", jid).put("cc", 1));
                        }
                        joblyLoginJson = joblyLoginJsonArrayGen.toString();
                        joblyLoginFlag = true;
                    }
                }
                //按时段新登数
                Integer hourlyNewLoginCounts = null == _jedis.get(hourlyNewLoginCountsKey) ? 0 : Integer.parseInt(_jedis.get(hourlyNewLoginCountsKey));
                if (!_jedis.sismember(new_char, uname)) {
                    hourlyNewLoginCounts++;
                    _jedis.set(hourlyNewLoginCountsKey, hourlyNewLoginCounts.toString());
                    _jedis.expire(hourlyNewLoginCountsKey, 24 * 60 * 60);
                    hourlyLoginFlag = true;
                }


                //新增ip数
                if (_jedis.sadd(tt_ip,ip) == 1l) {
                    _jedis.sadd(new_ip,ip);
                }

                //新增账号
                if (_jedis.sadd(tt_char,uname) == 1l) {
                    _jedis.sadd(new_char,uname);
                }

                _jedis.sadd(td_ip, ip);
                _jedis.sadd(td_char, uname);
                _jedis.expire(td_ip, 24 * 60 * 60);
                _jedis.expire(td_char, 24 * 60 * 60);

                Long daily_logins = _jedis.scard(td_ip);
                Long daily_logins_char = _jedis.scard(td_char);
                Long daily_logins_new = _jedis.scard(new_ip);
                Long daily_logins_char_new = _jedis.scard(new_char);

                System.out.println("=============" + PSG + "==============");
                System.out.println("登录ip：" + ip + " 登录账号：" + uname);
                System.out.println("登录ip数：" + daily_logins + " 登录人数：" + daily_logins_char);
                System.out.println("新增ip数：" + daily_logins_new + " 新增人数：" + daily_logins_char_new);
                System.out.println("======================================");

                Long up_time = System.currentTimeMillis() / 1000;
                //date 当天0点时间戳
                String datestr = todayStr + " 00:00:00";
                Long datetime = date.str2timestamp(datestr);

                String sql = "INSERT INTO `opdata_signinLogin_today` (`platform`,`server`,`date`,`daily_logins`,`daily_logins_char`," +
                        " `daily_logins_new`, `daily_logins_char_new`) VALUES(" + platform_id + "," + server_id + "," + datetime + "," + daily_logins + "," + daily_logins_char +
                        "," + daily_logins_new + "," + daily_logins_char_new + ") ON DUPLICATE KEY UPDATE `up_time`=" + up_time + ",`daily_logins`=" + daily_logins +
                        ",`daily_logins_char`=" + daily_logins_char + ",`daily_logins_new`=" + daily_logins_new +
                        ",`daily_logins_char_new`=" + daily_logins_char_new + ";";
                JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

                if (con.add(sql)) {
                    //用户登录信息
                    String key = "glogininfo-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname;
                    String value = "glogindetail-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname + "-" + datetime;
                    _jedis.rpush(key, value);
                    Map map = new HashMap();
                    map.put("loginip", ip);
                    map.put("isadult", "1");
                    map.put("client", "pc");
                    map.put("chid", chid);
                    map.put("chposid", chposid);
                    map.put("adplanning_id", adplanning_id);
                    map.put("chunion_subid", chunion_subid);
                    _jedis.hmset(value, map);
                    System.out.println("*********** Success ************");
                }

                if (hourlyLoginFlag || joblyLoginFlag) {
                    String hourlyLoginDbCol = "LT" + curHour;
                    String hourlyNewLoginDbCol = "NLT" + curHour;
                    String hourlyLoginSql = String.format("INSERT INTO opdata_signinLogin_hourly_today (platform, server, date, joblyLogins, %s, %s) " +
                            "VALUES (%s, %s, %d, '%s', %d, %d) ON DUPLICATE KEY UPDATE %s=%d, %s=%d, joblyLogins='%s';", hourlyLoginDbCol, hourlyNewLoginDbCol,
                            platform_id, server_id, datetime, joblyLoginJson, hourlyLoginCounts, hourlyNewLoginCounts, hourlyLoginDbCol, hourlyLoginCounts,
                            hourlyNewLoginDbCol, hourlyNewLoginCounts, joblyLoginJson);
System.out.println(hourlyLoginSql);
                    if (con.add(hourlyLoginSql)) {
                        System.out.println("*********** hourlyLoginSql Success ************");
                    }
                }
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("game_abbr","platform_id","server_id","datetime","up_time","mj_characters","js_characters","ds_characters","daily_logins","daily_logins_char","daily_logins_new","daily_logins_char_new","daily_characters"));
    }
}
