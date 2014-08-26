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

import java.security.NoSuchAlgorithmException;
import java.util.*;

public class GTaskBolt extends BaseBasicBolt {
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
        boolean isOnline = Boolean.parseBoolean(stormConf.get("isOnline").toString());
        if (isOnline) {
            _gamecfg = stormConf.get("gamecfg_path").toString();
        } else {
            _gamecfg = "/config/test.games.properties";
        }
        _prop = _cfgLoader.loadConfig(_gamecfg, isOnline);
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));

    }
    @Override
    public void execute(Tuple input , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);
        String sentence = input.getString(0);
        String[] logs = sentence.split("\\|");

        if (logs.length >= 10) {
            String game_abbr = logs[0];
            String platform = logs[1];
            String server = logs[2];
            String token = logs[3];
            String datetimestr = logs[4];
            String keywords = logs[5];
            String task_id = logs[6];
            String uname = "";
            String cname;
            Long proc1 = 0l;
            Long proc2 = 0l;
            Long proc3 = 0l;
            Long proc4 = 0l;
            Long proc5 = 0l;
            Long proc6 = 0l;
            Long proc7 = 0l;
            Long proc8 = 0l;
            Long proc9 = 0l;
            Long proc10 = 0l;
            Long taskfin = 0l;
            String update_sql = "";

            String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
            String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
            String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
            String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
            String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
            if (host != null) {

                //验证token
                String log_key = _prop.getProperty("game." + game_abbr + ".key");
                String raw_str = game_abbr + platform + server + log_key;
                String token_gen = "";
                try {
                    token_gen = new md5().gen_md5(raw_str);
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                }
                if (token_gen.equals(token)) {

                    //唯一键
                    String PSG = platform + ":" + server + ":" + game_abbr;
                    System.out.println("======================");
                    //接收任务
                    //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_recv|100101|z632954401|亂世逍遙|1
                    if (keywords.equals("task_recv") && logs.length == 10) {
                        uname = logs[7];
                        cname = logs[8];
                        String recv_key = "task:" + PSG + ":" + task_id + ":recv:set";
                        _jedis.sadd(recv_key, cname);
                        proc1 = _jedis.scard(recv_key);
                        update_sql = "`proc1`=" + proc1;
                        System.out.println("接收任务人数：" + proc1);
                    }
                    //更新任务
                    //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_update|100101|2|z632954401|亂世逍遙|3
                    else if (keywords.equals("task_update") && logs.length == 11) {
                        String steps = logs[7];
                        uname = logs[8];
                        cname = logs[9];
                        String update_key = "task:" + PSG + ":" + task_id + ":update:" + steps + ":set";
                        _jedis.sadd(update_key, cname);
                        Long update = _jedis.scard(update_key);
                        switch (Integer.parseInt(steps)) {
                            case 2: proc2 = update;
                            case 3: proc3 = update;
                            case 4: proc4 = update;
                            case 5: proc5 = update;
                            case 6: proc6 = update;
                            case 7: proc7 = update;
                            case 8: proc8 = update;
                            case 9: proc9 = update;
                            case 10: proc10 = update;
                        }
                        update_sql = "`proc" + steps + "`=" + update;
                        System.out.println("更新至" + steps + "步人数 : " + update);
                    }
                    //完成任务
                    //AHSG|100|1|08dffed798bf0fd8efaf167814e17460|2013-11-25 08:34:48|task_fin|100101|z632954401|亂世逍遙|5
                    else if (keywords.equals("task_fin") && logs.length == 10) {
                        uname = logs[7];
                        cname = logs[8];
                        String fin_key = "task:" + PSG + ":" + task_id + ":fin:set";
                        _jedis.sadd(fin_key, cname);
                        taskfin = _jedis.scard(fin_key);
                        update_sql = "`taskfin`=" + taskfin;
                        System.out.println("完成任务人数：" + taskfin);
                    }
                    System.out.println("======================");

                    //date 当天0点时间戳
                    String[] d = datetimestr.split(" ");
                    String tdstr = "";
                    if (d.length == 2) {
                        tdstr = d[0];
                        datetimestr = tdstr + " 00:00:00";
                    }
                    Long datetime = date.str2timestamp(datetimestr);

                    JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);
                    List<String> sqls = new ArrayList<String>();
                    String tbname = "gamedata_taskLost_todayup";

                    Map<String, Object> insert = new HashMap<String, Object>();
                    insert.put("platform", platform);
                    insert.put("server", server);
                    insert.put("date", datetime);
                    insert.put("if_new", 0);
                    insert.put("taskid", task_id);
                    insert.put("proc1", proc1);
                    insert.put("proc2", proc2);
                    insert.put("proc3", proc3);
                    insert.put("proc4", proc4);
                    insert.put("proc5", proc5);
                    insert.put("proc6", proc6);
                    insert.put("proc7", proc7);
                    insert.put("proc8", proc8);
                    insert.put("proc9", proc9);
                    insert.put("proc10", proc10);
                    insert.put("taskfin", taskfin);

                    Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
                    data.put("insert", insert);

                    String sql = con.setSql("insert", tbname, data) + " ON DUPLICATE KEY UPDATE " + update_sql;
                    sqls.add(sql);
                    //判断是否为新用户
                    String regkey = "greginfo-" + platform + "-" + game_abbr + "-" + server + "-" + uname;
                    if (_jedis.exists(regkey)) {
                        List userinfo = _jedis.lrange(regkey, 0, 0);
                        String[] info = userinfo.get(0).toString().split("-");
                        if (info.length == 6) {
                            String todayStr = date.timestamp2str(Long.parseLong(info[5]), "yyyy-MM-dd");
                            if (tdstr.equals(todayStr)) {
                                insert.put("if_new", 1);
                                Map<String, Map<String, Object>> data1 = new HashMap<String, Map<String, Object>>();
                                data1.put("insert", insert);
                                String sql1 = con.setSql("insert", tbname, data);
                                sql1 = sql1 + " ON DUPLICATE KEY UPDATE " + update_sql;
                                sqls.add(sql1);
                            }
                        }
                    }

                    //广告有效用户（完成新手任务）
                    if (keywords.equals("task_newbie") && logs.length == 9) {
                        uname = logs[6];
                        cname = logs[7];
                        //获取广告信息
                        String hash_key = "gadinfo-" + platform + "-" + uname;
                        if (_jedis.exists(hash_key)) {
                            List<String> gadinfo = _jedis.hmget(hash_key, "chid", "chposid", "adplanning_id", "chunion_subid");
                            String chid = gadinfo.get(0);
                            String chposid = gadinfo.get(1);
                            String adplanning_id = gadinfo.get(2);
                            String chunion_subid = gadinfo.get(3);

                            if (Integer.parseInt(adplanning_id) > 0) {
                                String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
                                Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

                                String effective_key = "adEffective:" + todayStr + ":" + game_abbr + ":" + platform + ":" + server + ":" + adplanning_id + ":" + chunion_subid + ":incr";
                                Integer countAdEffective = !_jedis.exists(effective_key) ? 0 : Integer.parseInt(_jedis.get(effective_key));
                                countAdEffective++;
                                _jedis.incr(effective_key);
                                _jedis.expire(effective_key, 24*60*60);
                                String adEffectiveSql = String.format("INSERT INTO adplanning_signinLogin_today (adplanning_id, chunion_subid, platform, server, date, up_time, effective)" +
                                                "VALUES (%s, %s, %s, %s, %d, %d, %d) ON DUPLICATE KEY UPDATE effective=%d",
                                        adplanning_id, chunion_subid, platform, server, todayDate, datetime, countAdEffective, countAdEffective);
                                sqls.add(adEffectiveSql);
                            }
                        }
                    }

                    if (con.batchAdd(sqls)) {
                        System.out.println("*********** Success ************");
                    }
                }
            }
        }
    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("word"));}
}