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

public class GTaskCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    private List<String> sqls = new ArrayList<String>();
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
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        //AHSG|100|1|2013-11-25 08:34:48|task_recv|100101|z632954401|亂世逍遙|1
        String game_abbr = tuple.getStringByField("game_abbr");
        String platform = tuple.getStringByField("platform");
        String server = tuple.getStringByField("server");
        String logtime = tuple.getStringByField("logtime");
        String keywords = tuple.getStringByField("keywords");
        String task_id = tuple.getStringByField("task_id");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String steps = tuple.getStringByField("steps");

        //2013-11-25 08:34:48
        Long logtimestamp = date.str2timestamp(logtime);
        //20131125
        String todayStr = date.timestamp2str(logtimestamp,"yyyyMMdd");
        //当天时间戳
        Long datetime = date.str2timestamp(todayStr);

        //唯一键
        String PSG = todayStr + ":" + platform + ":" + server + ":" + game_abbr + ":" + task_id;

        String recv_key = "task:" + PSG  + ":recv:set";
        String update_key = "task:" + PSG  + ":update:" + steps + ":set";
        String fin_key = "task:" + PSG  + ":fin:set";
        Map<String, Object> insert = new HashMap<String, Object>();
        Map<String, Object> update = new HashMap<String, Object>();

        System.out.println("==========="+PSG+"===========");


        //接收任务
        if (keywords.equals("task_recv")) {
            _jedis.sadd(recv_key, cname);
            _jedis.expire(recv_key,24*60*60);
            Long recv = _jedis.scard(recv_key);
            insert.put("proc1", recv);
            update.put("proc1", recv);
            System.out.println("接收任务人数：" + recv);
        }


        //更新任务
        if (keywords.equals("task_update")) {

            _jedis.sadd(update_key, cname);
            _jedis.expire(update_key,24*60*60);
            Long procn = _jedis.scard(update_key);
            insert.put("proc"+ steps, procn);
            update.put("proc"+ steps, procn);
            System.out.println("更新至" + steps + "步人数 : " + procn);
        }


        //完成任务
        if (keywords.equals("task_fin")) {

            _jedis.sadd(fin_key, cname);
            _jedis.expire(fin_key, 24 * 60 * 60);
            Long taskfin = _jedis.scard(fin_key);
            insert.put("taskfin", taskfin);
            update.put("taskfin", taskfin);
            System.out.println("完成任务人数：" + taskfin);
        }


        //广告有效用户（完成新手任务）
        if (keywords.equals("task_newbie")) {
            //获取广告信息
            String hash_key = "gadinfo-" + platform + "-" + uname;
            if (_jedis.exists(hash_key)) {
                List<String> gadinfo = _jedis.hmget(hash_key, "chid", "chposid", "adplanning_id", "chunion_subid");
                String chid = gadinfo.get(0);
                String chposid = gadinfo.get(1);
                String adplanning_id = gadinfo.get(2);
                String chunion_subid = gadinfo.get(3);

                if (Integer.parseInt(adplanning_id) > 0) {
                    String effective_key = "adEffective:" + todayStr + ":" + game_abbr + ":" + platform + ":" + server + ":" + adplanning_id + ":" + chunion_subid + ":incr";
                    Integer countAdEffective = !_jedis.exists(effective_key) ? 0 : Integer.parseInt(_jedis.get(effective_key));
                    countAdEffective++;
                    _jedis.incr(effective_key);
                    _jedis.expire(effective_key, 24*60*60);
                    String adEffectiveSql = String.format("INSERT INTO adplanning_signinLogin_today (adplanning_id, chunion_subid, platform, server, date, up_time, effective)" +
                                    "VALUES (%s, %s, %s, %s, %d, %d, %d) ON DUPLICATE KEY UPDATE effective=%d",
                            adplanning_id, chunion_subid, platform, server, datetime, logtimestamp, countAdEffective, countAdEffective);
                    sqls.add(adEffectiveSql);
                }
            }
        }

        String host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
        JdbcMysql con = JdbcMysql.getInstance(game_abbr, host, port, db, user, passwd);

        String tbname = "gamedata_taskLost_todayup";
        insert.put("platform", platform);
        insert.put("server", server);
        insert.put("date", datetime);
        insert.put("if_new", 0);
        insert.put("taskid", task_id);
        Map<String, Map<String, Object>> data = new HashMap<String, Map<String, Object>>();
        data.put("insert", insert);
        data.put("update",update);
        String sql = con.setSql("replace", tbname, data);
        sqls.add(sql);
        //判断是否为新用户
        String regkey = "greginfo-" + platform + "-" + game_abbr + "-" + server + "-" + uname;
        if (_jedis.exists(regkey)) {
            List userinfo = _jedis.lrange(regkey, 0, 0);
            String[] info = userinfo.get(0).toString().split("-");
            if (info.length == 6) {
                String tdstr = date.timestamp2str(Long.parseLong(info[5]), "yyyyMMdd");
                if (tdstr.equals(todayStr)) {
                    insert.put("if_new", 1);
                    Map<String, Map<String, Object>> data1 = new HashMap<String, Map<String, Object>>();
                    data1.put("insert", insert);
                    data1.put("update",update);
                    String sql1 = con.setSql("replace", tbname, data1);
                    sqls.add(sql1);
                }
            }
        }
        if (sqls.size() >= 20) {
            if(con.batchAdd(sqls)) {
                sqls.clear();
                System.out.println("*********** Success ************");
            }
        }

    }
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}