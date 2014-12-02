package storm.qule_mgame.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;
import storm.qule_util.mgame.system2client;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/11/26.
 */
public class MBillCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();
    private static mysql _dbconnect = null;

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    /**
     * 加载配置文件
     *
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

        _dbconnect = new mysql();

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        String bill_datetime = tuple.getStringByField("bill_datetime");
        String rechtime = tuple.getStringByField("rechtime");
        String order_id = tuple.getStringByField("order_id");
        String amount = tuple.getStringByField("amount");
        String isk = tuple.getStringByField("isk");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String ifsuccess = tuple.getStringByField("ifsuccess");
        String level = tuple.getStringByField("level");

        Long rechtime_int = Long.parseLong(rechtime);
        String rechdayStr = date.timestamp2str(rechtime_int, "yyyyMMdd");

        Long rechdayDate = date.str2timestamp(rechdayStr, "yyyyMMdd");

        //根据用户登录列表信息获取用户系统、版本号信息
        String system = "";
        String appver = "";
        String devid = "";
        String model = "";
        String resolution = "";
        String operator = "";
        String network = "";
        String clientip = "";
        String district = "";
        String osver = "";
        String osbuilder = "";
        String devtype = "";

        String mloginListKey = "mlogin:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        List mloginListLatest = _jedis.lrange(mloginListKey, -1, -1);       //获取最近登陆信息
        if (!mloginListLatest.isEmpty()) {
            String mloginLatestDetailKey = mloginListLatest.get(0).toString();
            List<String> mloginLatestDetail = _jedis.hmget(mloginLatestDetailKey, "os", "appver", "devid", "model", "resolution",
                    "operator", "network", "clientip", "district", "osver", "osbuilder", "devtype");
            if (!mloginLatestDetail.isEmpty()) {
                system = mloginLatestDetail.get(0);
                appver = mloginLatestDetail.get(1);
                devid = mloginLatestDetail.get(2);
                model = mloginLatestDetail.get(3);
                resolution = mloginLatestDetail.get(4);
                operator = mloginLatestDetail.get(5);
                network = mloginLatestDetail.get(6);
                clientip = mloginLatestDetail.get(7);
                district = mloginLatestDetail.get(8);
                osver = mloginLatestDetail.get(9);
                osbuilder = mloginLatestDetail.get(10);
                devtype = mloginLatestDetail.get(11);
            }
        }

        Integer client = system2client.turnSystem2ClientId(system);

        //记录充值记录缓存
        String mrechargeListKey = "mrecharge:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        String mrechargeListValue = "mrecharge:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":" +
                rechtime + ":record:detail";
        _jedis.rpush(mrechargeListKey, mrechargeListValue);

        Map mrechargeListDetailMap = new HashMap();
        mrechargeListDetailMap.put("os", system);
        mrechargeListDetailMap.put("appver", appver);
        mrechargeListDetailMap.put("name", cname);
        mrechargeListDetailMap.put("amount", amount);
        mrechargeListDetailMap.put("isk", isk);
        mrechargeListDetailMap.put("order_id", order_id);
        mrechargeListDetailMap.put("log_time", bill_datetime);
        mrechargeListDetailMap.put("ifsuccess", ifsuccess);
        mrechargeListDetailMap.put("devid", devid);
        mrechargeListDetailMap.put("model", model);
        mrechargeListDetailMap.put("resolution", resolution);
        mrechargeListDetailMap.put("operator", operator);
        mrechargeListDetailMap.put("network", network);
        mrechargeListDetailMap.put("clientip", clientip);
        mrechargeListDetailMap.put("district", district);
        mrechargeListDetailMap.put("osver", osver);
        mrechargeListDetailMap.put("osbuilder", osbuilder);
        mrechargeListDetailMap.put("devtype", devtype);
        _jedis.hmset(mrechargeListValue, mrechargeListDetailMap);

        //新玩家首日、首周、首月付费率
        Long rechargefd = 0L, rechamountfd = 0L, rechiskfd = 0L, rechargefw = 0L, rechamountfw = 0L, rechiskfw = 0L,
                rechargefm = 0L, rechamountfm = 0L, rechiskfm = 0L;
        if ("1".equals(ifsuccess)) {
            Long thisday_int;
            String thisdayStr;

            //当日充值用户列表
            String rechargeListDailyKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":account:set";
            _jedis.sadd(rechargeListDailyKey, uname);


            //新玩家首日付费列表
            String rechargefdListKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":account:fd:set";
            //新玩家首日付费金额
            String rechargefdAmountKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":amount:fd:incr";
            //新玩家首日付费游戏币
            String rechargefdIskKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":isk:fd:incr";
            //新玩家首周付费列表
            String rechargefwListKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":account:fw:set";
            //新玩家首周付费金额
            String rechargefwAmountKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":amount:fw:incr";
            //新玩家首周付费游戏币
            String rechargefwIskKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":isk:fw:incr";
            //新玩家首月付费列表
            String rechargefmListKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":account:fm:set";
            //新玩家首月付费金额
            String rechargefmAmountKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":amount:fm:incr";
            //新玩家首月付费游戏币
            String rechargefmIskKey = "mrecharge:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":isk:fm:incr";

            //1. 首日
            String loginAccListSvrNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + rechdayStr + ":" + appver + ":account:new:set";

            rechamountfd = !_jedis.exists(rechargefdAmountKey) ? 0L : Integer.parseInt(_jedis.get(rechargefdAmountKey));
            rechamountfw = !_jedis.exists(rechargefwAmountKey) ? 0L : Integer.parseInt(_jedis.get(rechargefwAmountKey));
            rechamountfm = !_jedis.exists(rechargefmAmountKey) ? 0L : Integer.parseInt(_jedis.get(rechargefmAmountKey));

            rechiskfd = !_jedis.exists(rechargefdIskKey) ? 0L : Integer.parseInt(_jedis.get(rechargefdIskKey));
            rechiskfw = !_jedis.exists(rechargefwIskKey) ? 0L : Integer.parseInt(_jedis.get(rechargefwIskKey));
            rechiskfm = !_jedis.exists(rechargefmIskKey) ? 0L : Integer.parseInt(_jedis.get(rechargefmIskKey));

            if (_jedis.sismember(loginAccListSvrNewKey, uname)) {
                _jedis.sadd(rechargefdListKey, uname);
                _jedis.sadd(rechargefwListKey, uname);
                _jedis.sadd(rechargefmListKey, uname);

                rechamountfd += Integer.parseInt(amount);
                rechamountfw += Integer.parseInt(amount);
                rechamountfm += Integer.parseInt(amount);

                rechiskfd += Integer.parseInt(isk);
                rechiskfw += Integer.parseInt(isk);
                rechiskfm += Integer.parseInt(isk);
            } else {
                //2. 首周
                boolean fwflag = false;
                for (int d = 1; d < 7; d++) {
                    thisday_int = rechdayDate - d * 24 * 60 * 60;
                    thisdayStr = date.timestamp2str(thisday_int, "yyyyMMdd");
                    loginAccListSvrNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                            ":" + thisdayStr + ":" + appver + ":account:new:set";

                    if (_jedis.sismember(loginAccListSvrNewKey, uname)) {
                        _jedis.sadd(rechargefwListKey, uname);
                        _jedis.sadd(rechargefmListKey, uname);

                        rechamountfw += Integer.parseInt(amount);
                        rechamountfm += Integer.parseInt(amount);

                        rechiskfw += Integer.parseInt(isk);
                        rechiskfm += Integer.parseInt(isk);

                        fwflag = true;
                    }
                }
                if (!fwflag) {
                    //3. 首月
                    for (int d = 8; d < 30; d++) {
                        thisday_int = rechdayDate - d * 24 * 60 * 60;
                        thisdayStr = date.timestamp2str(thisday_int, "yyyyMMdd");
                        loginAccListSvrNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                                ":" + thisdayStr + ":" + appver + ":account:new:set";

                        if (_jedis.sismember(loginAccListSvrNewKey, uname)) {
                            _jedis.sadd(rechargefmListKey, uname);

                            rechamountfm += Integer.parseInt(amount);

                            rechiskfm += Integer.parseInt(isk);
                        }
                    }
                }
            }

            _jedis.set(rechargefdAmountKey, rechamountfd.toString());
            _jedis.set(rechargefwAmountKey, rechamountfw.toString());
            _jedis.set(rechargefmAmountKey, rechamountfm.toString());

            _jedis.set(rechargefdIskKey, rechiskfd.toString());
            _jedis.set(rechargefwIskKey, rechiskfw.toString());
            _jedis.set(rechargefmIskKey, rechiskfm.toString());

            rechargefd = !_jedis.exists(rechargefdListKey) ? 0L : _jedis.scard(rechargefdListKey);

            rechargefw = !_jedis.exists(rechargefwListKey) ? 0L : _jedis.scard(rechargefwListKey);

            rechargefm = !_jedis.exists(rechargefmListKey) ? 0L : _jedis.scard(rechargefmListKey);
        }


        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;
            String sqls = "";

            String inssql_rechargedata = String.format("INSERT INTO recharge_order (client, platform, server, version," +
                    "datetime, order_id, status, account, cname, amount, isk, devid, os, appver, model, resolution," +
                    "operator, network, clientip, district, osver, osbuilder, devtype, level) VALUES (%d, %s, %s, %s, %s, " +
                    "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE " +
                    "amount=%s, isk=%s;", client, platform_id, server_id, appver, rechtime, order_id, ifsuccess,
                    uname, cname, amount, isk, devid, system, appver, model, resolution, operator, network, clientip,
                    district, osver, osbuilder, devtype, level, amount, isk);

            String inssql_signinlogindaily = "";
            if ("1".equals(ifsuccess)) {
                inssql_signinlogindaily = String.format("INSERT INTO signinlogindaily (client, platform, server," +
                "date, version, rechargefd, rechamountfd, rechiskfd, rechargefw, rechamountfw, rechiskfw, rechargefm," +
                "rechamountfm, rechiskfm) VALUES (%d, %s, %s, %s, %s, %d, %d, %d, %d, %d, %d, %d, %d, %d) ON DUPLICATE" +
                "KEY UPDATE rechargefd=%d, rechamountfd=%d, rechiskfd=%d, rechargefw=%d, rechamountfw=%d, rechiskfw=%d," +
                "rechargefm=%d, rechamountfm=%d, rechiskfm=%d;", client, platform_id, server_id, rechtime, appver,
                rechargefd, rechamountfd, rechiskfd, rechargefw, rechamountfw, rechiskfw, rechargefm, rechamountfm,
                rechiskfm, rechargefd, rechamountfd, rechiskfd, rechargefw, rechamountfw, rechiskfw, rechargefm,
                rechamountfm, rechiskfm);
            }

            sqls += inssql_rechargedata;
            sqls += inssql_signinlogindaily;
System.out.println(sqls);

            try {
                sql_ret = _dbconnect.DirectUpdateBatch(game_abbr, sqls);
            } catch (SQLException e) {
                e.printStackTrace();
            }
System.out.println("|||||-----batch update result: " + sql_ret);
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}