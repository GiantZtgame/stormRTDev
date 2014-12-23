package storm.qule_mgame.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;
import storm.qule_util.mgame.platform2ifabroad;
import storm.qule_util.mgame.system2client;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/11/25.
 */
public class MLoginCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();
    private static mysql _dbconnect = null;

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static timerFlushDb _dbFlushTimer = new timerFlushDb();
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
        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")), 11);

        _dbconnect = new mysql();

    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        String login_datetime = tuple.getStringByField("login_datetime");
        String devid = tuple.getStringByField("devid");
        String uname = tuple.getStringByField("uname");
        String system = tuple.getStringByField("system");
        String appver = tuple.getStringByField("appver");
        String model = tuple.getStringByField("model");
        String resolution = tuple.getStringByField("resolution");
        String sp = tuple.getStringByField("sp");
        String network = tuple.getStringByField("network");
        String client_ip = tuple.getStringByField("client_ip");
        String district = tuple.getStringByField("district");
        String osver = tuple.getStringByField("osver");
        String osbuilder = tuple.getStringByField("osbuilder");
        String devtype = tuple.getStringByField("devtype");
        Integer ifnew = 0;

        Long login_datetime_int = Long.parseLong(login_datetime);
        String todayStr = date.timestamp2str(login_datetime_int, "yyyyMMdd");
        String todayHourStr = date.timestamp2str(login_datetime_int, "yyyyMMdd-HH");
        String todayMinuteStr = date.timestamp2str(login_datetime_int, "yyyyMMdd-HHmm");
        String curHour = date.timestamp2str(login_datetime_int, "H");

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");
        Long todayMinuteDate = date.str2timestamp(todayMinuteStr, "yyyyMMdd-HHmm");

        Integer client = system2client.turnSystem2ClientId(system);
        Integer ifAbroad = platform2ifabroad.turnPlatform2ifabroad(platform_id);

        boolean ifDau = false, ifRech = false;

        //记录登陆列表
        String mloginListKey = "mlogin:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        String mloginListValue = "mlogin:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":" +
                login_datetime + ":record:detail";
        _jedis.rpush(mloginListKey, mloginListValue);

        Map mloginListDetailMap = new HashMap();
        mloginListDetailMap.put("devid", devid);
        mloginListDetailMap.put("os", system);
        mloginListDetailMap.put("appver", appver);
        mloginListDetailMap.put("model", model);
        mloginListDetailMap.put("resolution", resolution);
        mloginListDetailMap.put("operator", sp);
        mloginListDetailMap.put("network", network);
        mloginListDetailMap.put("clientip", client_ip);
        mloginListDetailMap.put("district", district);
        mloginListDetailMap.put("osver", osver);
        mloginListDetailMap.put("osbuilder", osbuilder);
        mloginListDetailMap.put("devtype", devtype);
        _jedis.hmset(mloginListValue, mloginListDetailMap);


        //全局累计数据key
        String overallKey = "overalldata:" + game_abbr + ":" + system + ":" + platform_id + ":hash:incr";
        String overallAbroadKey = "overalldata:" + game_abbr + ":" + system + ":" + ifAbroad + ":hash:abroad:incr";


        //1. 各系统所有登陆账号列表
        String loginAccListKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":total:account:set";
        _jedis.sadd(loginAccListKey, uname);

        Long overalldata_accounts = _jedis.scard(loginAccListKey);

        _jedis.hset(overallKey, "accounts", overalldata_accounts.toString());


        //2. 各系统当天所有登陆账号列表
        String loginAccListDailyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr +
                ":account:set";
        _jedis.sadd(loginAccListDailyKey, uname);
        _jedis.expire(loginAccListDailyKey, 60 * 24 * 60 * 60);

        Long overalldatadaily_dau = _jedis.scard(loginAccListDailyKey);

        _jedis.hset(overallKey, "accounts:"+todayStr, overalldatadaily_dau.toString());


        //3. 各系统当天各版本所有登陆账号列表
        String loginAccListDailyVerlyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr +
                ":" + appver + ":account:set";
        _jedis.sadd(loginAccListDailyVerlyKey, uname);
        _jedis.expire(loginAccListDailyVerlyKey, 60 * 24 * 60 * 60);

        Long overalldatadailyverly_dau = _jedis.scard(loginAccListDailyVerlyKey);

        _jedis.hset(overallKey, "accounts:"+todayStr+":"+appver, overalldatadailyverly_dau.toString());


        //4. 各系统当天各区服各版本登陆账号列表
        String loginAccListSvrDailyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":account:set";
        _jedis.sadd(loginAccListSvrDailyKey, uname);
        _jedis.expire(loginAccListSvrDailyKey, 60 * 24 * 60 * 60);

        Long signinlogindaily_logins = _jedis.scard(loginAccListSvrDailyKey);


        //5. 各系统当天各时段各区服各版本登陆账号列表
        String loginAccListSvrHourlyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayHourStr + ":" + appver + ":account:set";
        _jedis.sadd(loginAccListSvrHourlyKey, uname);
        _jedis.expire(loginAccListSvrHourlyKey, 60 * 24 * 60 * 60);

        Long signinloginhourly_logins = _jedis.scard(loginAccListSvrHourlyKey);



        String loginAccListSvrKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":account:set";
        //6. 各系统当天各区服各版本新增账号列表
        String loginAccListSvrNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":account:new:set";
        //10. 各系统当天各时段各区服各版本新增账号列表
        String loginAccListSvrHourlyNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayHourStr + ":" + appver + ":account:new:set";
        //11. 各系统当天各时段各分钟各区服各版本新增账号列表
        String loginAccListSvrMinlyNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayMinuteStr + ":" + appver + ":account:new:set";
        //12. 各系统当天所有新登账号列表
        String loginAccListNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr + ":account:new:set";
        //14. 各系统当天各版本所有新登账号列表
        String loginAccListDailyVerlyNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayStr +
                ":" + appver + ":account:new:set";
        if (1L == _jedis.sadd(loginAccListSvrKey, uname)) {
            _jedis.sadd(loginAccListSvrNewKey, uname);
            _jedis.expire(loginAccListSvrNewKey, 60 * 24 * 60 * 60);
            _jedis.sadd(loginAccListSvrHourlyNewKey, uname);
            _jedis.expire(loginAccListSvrHourlyNewKey, 60 * 24 * 60 * 60);
            _jedis.sadd(loginAccListSvrMinlyNewKey, uname);
            _jedis.expire(loginAccListSvrMinlyNewKey, 60 * 24 * 60 * 60);
            _jedis.sadd(loginAccListNewKey, uname);
            _jedis.sadd(loginAccListDailyVerlyNewKey, uname);
            _jedis.expire(loginAccListDailyVerlyNewKey, 60 * 24 * 60 * 60);

            ifnew = 1;
        }

        Long signinlogindaily_newacc = !_jedis.exists(loginAccListSvrNewKey) ? 0L : _jedis.scard(loginAccListSvrNewKey);
        Long signinloginhourly_newacc = !_jedis.exists(loginAccListSvrHourlyNewKey) ? 0L : _jedis.scard(loginAccListSvrHourlyNewKey);
        Long newonlinert_newacc = !_jedis.exists(loginAccListSvrMinlyNewKey) ? 0L : _jedis.scard(loginAccListSvrMinlyNewKey);
        Long overalldatadaily_daunew = !_jedis.exists(loginAccListNewKey) ? 0L : _jedis.scard(loginAccListNewKey);
        Long overalldatadailyverly_daunew = !_jedis.exists(loginAccListDailyVerlyNewKey) ? 0L : _jedis.scard(loginAccListDailyVerlyNewKey);

        //13. 各系统当天各时段所有登陆账号列表
        String loginAccListHourlyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + todayHourStr +
                ":account:set";
        _jedis.sadd(loginAccListHourlyKey, uname);
        _jedis.expire(loginAccListHourlyKey, 60 * 24 * 60 * 60);

        Long overalldatahourly_hau = _jedis.scard(loginAccListHourlyKey);


        //15. 各区服各客户端各版本当日充值用户登陆列表
        String mrechargeListKey = "mrecharge:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        String mloginRechListKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":rechargers:set";
        if (_jedis.exists(mrechargeListKey)) {
            _jedis.sadd(mloginRechListKey, uname);
            _jedis.expire(mloginRechListKey, 60 * 24 * 60 * 60);
        }



        // ===============新用户、活跃用户留存===============
        String DailyremainSql = "";
        int dayslist [] = {1,2,3,4,5,6,13,29};
        for (int day : dayslist) {
            Long thisdatetime_ts = login_datetime_int - day * 24 * 60 * 60;
            String thisdayStr = date.timestamp2str(thisdatetime_ts, "yyyyMMdd");
            Long thisdayTs = date.str2timestamp(thisdayStr);

            String dailyremainNewKey = "mNewRemain:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    thisdayStr + ":" + appver + ":" + day + ":account:set";
            String dailyremainAuKey = "mAuRemain:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    thisdayStr + ":" + appver + ":" + day + ":account:set";
            String thisdayNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + thisdayStr + ":" + appver + ":account:new:set";
            String thisdayLoginKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                    ":" + thisdayStr + ":" + appver + ":account:set";

            if (_jedis.sismember(thisdayNewKey, uname)) {
                _jedis.sadd(dailyremainNewKey, uname);
                _jedis.expire(dailyremainNewKey, 60 * 24 * 60 * 60);
            }

            if (_jedis.sismember(thisdayLoginKey, uname)) {
                _jedis.sadd(dailyremainAuKey, uname);
                _jedis.expire(dailyremainAuKey, 60 * 24 * 60 * 60);
            }

            Integer dailyremainDay = day + 1;
            String newColname = "newd" + dailyremainDay.toString();
            String auColname = "daud" + dailyremainDay.toString();

            Long thisdayNewRemain = !_jedis.exists(dailyremainNewKey) ? 0L : _jedis.scard(dailyremainNewKey);
            Long thisdayAuRemain = !_jedis.exists(dailyremainAuKey) ? 0L : _jedis.scard(dailyremainAuKey);
            String thisdaySql = String.format("INSERT INTO dailyremain (client, platform, server, date, version, %s, %s)" +
                    " VALUES (%d, '%s', '%s', %d, '%s', %d, %d) ON DUPLICATE KEY UPDATE %s=%d, %s=%d;", newColname,
                    auColname, client, platform_id, server_id, thisdayTs, appver, thisdayNewRemain, thisdayAuRemain,
                    newColname, thisdayNewRemain, auColname, thisdayAuRemain);
            DailyremainSql += thisdaySql;
        }




        // ================7日，14日，30日活跃================
        // ================7日，14日，30日流失================

        //7日活跃
        String mactived7Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":au:d7:set";
        //14日活跃
        String mactived14Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":au:d14:set";
        //30日活跃
        String mactived30Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":au:d30:set";

        String inssql_mactivedaily = "";
        String inssql_mlostretdaily = "";

        String mactiveCalcFlagKey = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                appver + ":au:flag";
        String mactiveCalcFlagVal = !_jedis.exists(mactiveCalcFlagKey) ? "" : _jedis.get(mactiveCalcFlagKey);
        if (!mactiveCalcFlagVal.equals(todayStr)) {
            _jedis.set(mactiveCalcFlagKey, todayStr);

            //7日活跃
            //String mactived7Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
            //        todayStr + ":" + appver + ":au:d7:set";
            String mactived7NewKey = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":au:d7:new:set";
            for (int d=1 ; d<=7; d++) {
                Long thisdayTs = todayDate - d * 24 * 60 * 60;
                String thisdayStr = date.timestamp2str(thisdayTs, "yyyyMMdd");
                String loginAccListSvrThisdayKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:set";

                _jedis.sunionstore(mactived7Key, loginAccListSvrThisdayKey, mactived7Key);

                String loginAccListSvrThisdayNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:new:set";

                _jedis.sunionstore(mactived7NewKey, loginAccListSvrThisdayNewKey, mactived7NewKey);
            }
            Long mactived7 = !_jedis.exists(mactived7Key) ? 0L : _jedis.scard(mactived7Key);
            _jedis.expire(mactived7Key, 7 * 24 * 60 * 60);

            Long mactived7new = !_jedis.exists(mactived7NewKey) ? 0L : _jedis.scard(mactived7NewKey);
            _jedis.expire(mactived7NewKey, 7 * 24 * 60 * 60);

            //14日活跃
            //String mactived14Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
            //        todayStr + ":" + appver + ":au:d14:set";
            String mactived14NewKey = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":au:d14:new:set";

            _jedis.sunionstore(mactived14Key, mactived7Key, mactived14Key);
            _jedis.sunionstore(mactived14NewKey, mactived7NewKey, mactived14NewKey);

            for (int d=8 ; d<=14; d++) {
                Long thisdayTs = todayDate - d * 24 * 60 * 60;
                String thisdayStr = date.timestamp2str(thisdayTs, "yyyyMMdd");
                String loginAccListSvrThisdayKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:set";

                _jedis.sunionstore(mactived14Key, loginAccListSvrThisdayKey, mactived14Key);

                String loginAccListSvrThisdayNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:new:set";

                _jedis.sunionstore(mactived14NewKey, loginAccListSvrThisdayNewKey, mactived14NewKey);
            }
            Long mactived14 = !_jedis.exists(mactived14Key) ? 0L : _jedis.scard(mactived14Key);
            _jedis.expire(mactived14Key, 7 * 24 * 60 * 60);

            Long mactived14new = !_jedis.exists(mactived14NewKey) ? 0L : _jedis.scard(mactived14NewKey);
            _jedis.expire(mactived14NewKey, 7 * 24 * 60 * 60);


            //30日活跃
            //String mactived30Key = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
            //        todayStr + ":" + appver + ":au:d30:set";
            String mactived30NewKey = "mactive:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":au:d30:new:set";

            _jedis.sunionstore(mactived30Key, mactived14Key, mactived30Key);
            _jedis.sunionstore(mactived30NewKey, mactived14NewKey, mactived30NewKey);

            for (int d=15 ; d<=30; d++) {
                Long thisdayTs = todayDate - d * 24 * 60 * 60;
                String thisdayStr = date.timestamp2str(thisdayTs, "yyyyMMdd");
                String loginAccListSvrThisdayKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:set";

                _jedis.sunionstore(mactived30Key, loginAccListSvrThisdayKey, mactived30Key);

                String loginAccListSvrThisdayNewKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                        server_id + ":" + thisdayStr + ":" + appver + ":account:new:set";

                _jedis.sunionstore(mactived30NewKey, loginAccListSvrThisdayNewKey, mactived30NewKey);
            }
            Long mactived30 = !_jedis.exists(mactived30Key) ? 0L : _jedis.scard(mactived30Key);
            _jedis.expire(mactived30Key, 7 * 24 * 60 * 60);

            Long mactived30new = !_jedis.exists(mactived30NewKey) ? 0L : _jedis.scard(mactived30NewKey);
            _jedis.expire(mactived30NewKey, 7 * 24 * 60 * 60);

            inssql_mactivedaily = String.format("INSERT INTO activedaily (client, platform, server, date, version, wau," +
                    "waunew, mau, maunew) VALUES (%d, '%s', '%s', %d, '%s', %d, %d, %d, %d) ON DUPLICATE KEY UPDATE " +
                    "wau=%d, waunew=%d, mau=%d, maunew=%d;", client, platform_id, server_id, todayDate, appver,
                    mactived7, mactived7new, mactived30, mactived30new, mactived7, mactived7new, mactived30,
                    mactived30new);


            //7日流失
            Long lostd7Date = todayDate - 8 * 24 * 60 * 60;
            String lostDay7Str = date.timestamp2str(lostd7Date, "yyyyMMdd");
            String loginAccListSvrlostDay7Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                    server_id + ":" + lostDay7Str + ":" + appver + ":account:set";

            String lostListd7Key = "mlost:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    lostDay7Str + ":" + appver + ":account:d7:set";

            _jedis.sdiffstore(lostListd7Key, loginAccListSvrlostDay7Key, mactived7Key);
            Long mlostd7 = !_jedis.exists(lostListd7Key) ? 0L : _jedis.scard(lostListd7Key);
            _jedis.expire(lostListd7Key, 7 * 24 * 60 * 60);

            inssql_mlostretdaily = String.format("INSERT INTO lostretdaily (client, platform, server, date, version," +
                    "lostd7) VALUES (%d, '%s', '%s', %d, '%s', %d) ON DUPLICATE KEY UPDATE lostd7=%d;", client,
                    platform_id, server_id, lostd7Date, appver, mlostd7, mlostd7);

            //14日流失
            Long lostd14Date = todayDate - 15 * 24 * 60 * 60;
            String lostDay14Str = date.timestamp2str(lostd14Date, "yyyyMMdd");
            String loginAccListSvrlostDay14Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                    server_id + ":" + lostDay14Str + ":" + appver + ":account:set";

            String lostListd14Key = "mlost:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    lostDay7Str + ":" + appver + ":account:d14:set";

            _jedis.sdiffstore(lostListd14Key, loginAccListSvrlostDay14Key, mactived14Key);
            Long mlostd14 = !_jedis.exists(lostListd14Key) ? 0L : _jedis.scard(lostListd14Key);
            _jedis.expire(lostListd14Key, 7 * 24 * 60 * 60);

            inssql_mlostretdaily += String.format("INSERT INTO lostretdaily (client, platform, server, date, version," +
                    "lostd14) VALUES (%d, '%s', '%s', %d, '%s', %d) ON DUPLICATE KEY UPDATE lostd14=%d;", client,
                    platform_id, server_id, lostd14Date, appver, mlostd14, mlostd14);

            //30日流失
            Long lostd30Date = todayDate - 31 * 24 * 60 * 60;
            String lostDay30Str = date.timestamp2str(lostd30Date, "yyyyMMdd");
            String loginAccListSvrlostDay30Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                    server_id + ":" + lostDay30Str + ":" + appver + ":account:set";

            String lostListd30Key = "mlost:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    lostDay30Str + ":" + appver + ":account:d30:set";

            _jedis.sdiffstore(lostListd30Key, loginAccListSvrlostDay30Key, mactived30Key);
            Long mlostd30 = !_jedis.exists(lostListd30Key) ? 0L : _jedis.scard(lostListd30Key);
            _jedis.expire(lostListd30Key, 7 * 24 * 60 * 60);

            inssql_mlostretdaily += String.format("INSERT INTO lostretdaily (client, platform, server, date, version," +
                    "lostd30) VALUES (%d, '%s', '%s', %d, '%s', %d) ON DUPLICATE KEY UPDATE lostd30=%d;", client,
                    platform_id, server_id, lostd14Date, appver, mlostd30, mlostd30);
        }




        // ================7日，14日，30日回归================
        String rechKey = "mrecharge:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        // 7日回归活跃(登陆)用户
        Long retd7Date = todayDate - 8 * 24 * 60 * 60;
        String retd7Str = date.timestamp2str(retd7Date, "yyyyMMdd");
        String loginAccListSvrretDay7Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + retd7Str + ":" + appver + ":account:set";
        String retListd7DauKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":dau:d7:set";
        if (_jedis.sismember(loginAccListSvrretDay7Key, uname) && !_jedis.sismember(mactived7Key, uname)) {
            _jedis.sadd(retListd7DauKey, uname);
            _jedis.expire(retListd7DauKey, 60 * 24 * 60 * 60);
        }
        Long retListd7Dau = !_jedis.exists(retListd7DauKey) ? 0L : _jedis.scard(retListd7DauKey);

        // 7日回归充值用户
        String retListd7RechKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":rechargers:d7:set";
        if (_jedis.exists(rechKey) && _jedis.sismember(loginAccListSvrretDay7Key, uname) &&
                !_jedis.sismember(mactived7Key, uname)) {
            _jedis.sadd(retListd7RechKey, uname);
            _jedis.expire(retListd7RechKey, 60 * 24 * 60 * 60);
        }
        Long retListd7Rech = !_jedis.exists(retListd7RechKey) ? 0L : _jedis.scard(retListd7RechKey);

        // 14日回归活跃(登陆)用户
        Long retd14Date = todayDate - 15 * 24 * 60 * 60;
        String retd14Str = date.timestamp2str(retd14Date, "yyyyMMdd");
        String loginAccListSvrretDay14Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + retd14Str + ":" + appver + ":account:set";
        String retListd14DauKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":dau:d14:set";
        if (_jedis.sismember(loginAccListSvrretDay14Key, uname) && !_jedis.sismember(mactived14Key, uname)) {
            _jedis.sadd(retListd14DauKey, uname);
            _jedis.expire(retListd14DauKey, 60 * 24 * 60 * 60);
        }
        Long retListd14Dau = !_jedis.exists(retListd14DauKey) ? 0L : _jedis.scard(retListd14DauKey);

        // 14日回归充值用户
        String retListd14RechKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":rechargers:d14:set";
        if (_jedis.exists(rechKey) && _jedis.sismember(loginAccListSvrretDay14Key, uname) &&
                !_jedis.sismember(mactived14Key, uname)) {
            _jedis.sadd(retListd14RechKey, uname);
            _jedis.expire(retListd14RechKey, 60 * 24 * 60 * 60);
        }
        Long retListd14Rech = !_jedis.exists(retListd14RechKey) ? 0L : _jedis.scard(retListd7RechKey);

        // 30日回归活跃(登陆)用户
        Long retd30Date = todayDate - 31 * 24 * 60 * 60;
        String retd30Str = date.timestamp2str(retd30Date, "yyyyMMdd");
        String loginAccListSvrretDay30Key = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + retd30Str + ":" + appver + ":account:set";
        String retListd30DauKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":dau:d30:set";
        if (_jedis.sismember(loginAccListSvrretDay30Key, uname) && !_jedis.sismember(mactived30Key, uname)) {
            _jedis.sadd(retListd30DauKey, uname);
            _jedis.expire(retListd30DauKey, 60 * 24 * 60 * 60);
        }
        Long retListd30Dau = !_jedis.exists(retListd30DauKey) ? 0L : _jedis.scard(retListd30DauKey);

        // 30日回归充值用户
        String retListd30RechKey = "mreturn:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":rechargers:d30:set";
        if (_jedis.exists(rechKey) && _jedis.sismember(loginAccListSvrretDay30Key, uname) &&
                !_jedis.sismember(mactived30Key, uname)) {
            _jedis.sadd(retListd30RechKey, uname);
            _jedis.expire(retListd30RechKey, 60 * 24 * 60 * 60);
        }
        Long retListd30Rech = !_jedis.exists(retListd30RechKey) ? 0L : _jedis.scard(retListd30RechKey);




        //用户习惯 - 使用次数
        String mlogintimesNewaccKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":logintimes:newacc:incr";
        String mlogintimesDauKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":logintimes:dau:incr";
        String mlogintimesRechKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":logintimes:rechargers:incr";

        Long mlogintimesNewacc = !_jedis.exists(mlogintimesNewaccKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesNewaccKey));
        Long mlogintimesDau = !_jedis.exists(mlogintimesDauKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesDauKey));
        Long mlogintimesRech = !_jedis.exists(mlogintimesRechKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesRechKey));


        String perNewaccLogintimesDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + todayStr + ":" + appver + ":logintimes_perday:";
        String perNewaccLogintimesDistLatterKey = ":newacc:set";

        if (_jedis.sismember(loginAccListSvrNewKey, uname)) {
            mlogintimesNewacc = _jedis.incr(mlogintimesNewaccKey);
            _jedis.expire(mlogintimesNewaccKey, 60 * 24 * 60 * 60);


            //新用户对应游戏次数
            String perNewaccLogintimesKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":newacc_logintimes_perday:incr";
            Long thisNewAccLogintimes = _jedis.incr(perNewaccLogintimesKey);
            _jedis.expire(perNewaccLogintimesKey, 60 * 24 * 60 * 60);

            String perNewaccLogintimesDistKey = "";
            for (Integer i=1 ; i<=7 ; i++) {
                perNewaccLogintimesDistKey = perNewaccLogintimesDistFormerKey + i.toString() +
                        perNewaccLogintimesDistLatterKey;
                if (1 == _jedis.srem(perNewaccLogintimesDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisNewAccLogintimes <= 1) {
                segmentid = 1;
            } else if (thisNewAccLogintimes > 1 && thisNewAccLogintimes <= 3) {
                segmentid = 2;
            } else if (thisNewAccLogintimes > 3 && thisNewAccLogintimes <= 5) {
                segmentid = 3;
            } else if (thisNewAccLogintimes > 5 && thisNewAccLogintimes <= 10) {
                segmentid = 4;
            } else if (thisNewAccLogintimes > 10 && thisNewAccLogintimes <= 20) {
                segmentid = 5;
            } else if (thisNewAccLogintimes > 20 && thisNewAccLogintimes <= 50) {
                segmentid = 6;
            } else if (thisNewAccLogintimes > 50) {
                segmentid = 7;
            }
            if (0 != segmentid) {
                _jedis.sadd(perNewaccLogintimesDistFormerKey + segmentid.toString() + perNewaccLogintimesDistLatterKey,
                        uname);
                _jedis.expire(perNewaccLogintimesDistFormerKey + segmentid.toString() + perNewaccLogintimesDistLatterKey,
                        60 * 24 * 60 * 60);
            }

        }

        String perDauLogintimesDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + todayStr + ":" + appver + ":logintimes_perday:";
        String perDauLogintimesDistLatterKey = ":dau:set";

        if (_jedis.sismember(loginAccListSvrKey, uname)) {
            mlogintimesDau = _jedis.incr(mlogintimesDauKey);
            _jedis.expire(mlogintimesDauKey, 60 * 24 * 60 * 60);


            //活跃用户对应游戏次数
            String perDauLogintimesKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":dau_logintimes_perday:incr";
            Long thisDauLogintimes =  _jedis.incr(perDauLogintimesKey);
            _jedis.expire(perDauLogintimesKey, 60 * 24 * 60 * 60);

            String perDauLogintimesDistKey = "";
            for (Integer i=1 ; i<=7 ; i++) {
                perDauLogintimesDistKey = perDauLogintimesDistFormerKey + i.toString() +
                        perDauLogintimesDistLatterKey;
                if (1 == _jedis.srem(perDauLogintimesDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisDauLogintimes <= 1) {
                segmentid = 1;
            } else if (thisDauLogintimes > 1 && thisDauLogintimes <= 3) {
                segmentid = 2;
            } else if (thisDauLogintimes > 3 && thisDauLogintimes <= 5) {
                segmentid = 3;
            } else if (thisDauLogintimes > 5 && thisDauLogintimes <= 10) {
                segmentid = 4;
            } else if (thisDauLogintimes > 10 && thisDauLogintimes <= 20) {
                segmentid = 5;
            } else if (thisDauLogintimes > 20 && thisDauLogintimes <= 50) {
                segmentid = 6;
            } else if (thisDauLogintimes > 50) {
                segmentid = 7;
            }
            if (0 != segmentid) {
                _jedis.sadd(perDauLogintimesDistFormerKey + segmentid.toString() + perDauLogintimesDistLatterKey,
                        uname);
                _jedis.expire(perDauLogintimesDistFormerKey + segmentid.toString() + perDauLogintimesDistLatterKey,
                        60 * 24 * 60 * 60);
            }

            ifDau = true;
        }

        String perRechLogintimesDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" +
                server_id + ":" + todayStr + ":" + appver + ":logintimes_perday:";
        String perRechLogintimesDistLatterKey = ":rechargers:set";

        if (_jedis.sismember(mrechargeListKey, uname)) {
            mlogintimesRech = _jedis.incr(mlogintimesRechKey);
            _jedis.expire(mlogintimesRechKey, 60 * 24 * 60 * 60);


            //充值用户对应游戏次数
            String perRechLogintimesKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":rechargers_logintimes_perday:incr";
            Long thisRechLogintimes =  _jedis.incr(perRechLogintimesKey);
            _jedis.expire(perRechLogintimesKey, 60 * 24 * 60 * 60);

            String perRechLogintimesDistKey = "";
            for (Integer i=1 ; i<=7 ; i++) {
                perRechLogintimesDistKey = perRechLogintimesDistFormerKey + i.toString() +
                        perRechLogintimesDistLatterKey;
                if (1 == _jedis.srem(perRechLogintimesDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisRechLogintimes <= 1) {
                segmentid = 1;
            } else if (thisRechLogintimes > 1 && thisRechLogintimes <= 3) {
                segmentid = 2;
            } else if (thisRechLogintimes > 3 && thisRechLogintimes <= 5) {
                segmentid = 3;
            } else if (thisRechLogintimes > 5 && thisRechLogintimes <= 10) {
                segmentid = 4;
            } else if (thisRechLogintimes > 10 && thisRechLogintimes <= 20) {
                segmentid = 5;
            } else if (thisRechLogintimes > 20 && thisRechLogintimes <= 50) {
                segmentid = 6;
            } else if (thisRechLogintimes > 50) {
                segmentid = 7;
            }
            if (0 != segmentid) {
                _jedis.sadd(perRechLogintimesDistFormerKey + segmentid.toString() + perRechLogintimesDistLatterKey,
                        uname);
                _jedis.expire(perRechLogintimesDistFormerKey + segmentid.toString() + perRechLogintimesDistLatterKey,
                        60 * 24 * 60 * 60);
            }

            ifRech = true;
        }

        Long loginAccNewToday = !_jedis.exists(loginAccListSvrNewKey) ? 0L : _jedis.scard(loginAccListSvrNewKey);
        Long avg_logintimes_newacc = 0L==loginAccNewToday ? 0L : (mlogintimesNewacc / loginAccNewToday);

        Long loginAccToday = !_jedis.exists(loginAccListSvrDailyKey) ? 0L : _jedis.scard(loginAccListSvrDailyKey);
        Long avg_logintimes_dau = 0L==loginAccToday ? 0L : (mlogintimesDau / loginAccToday);

        Long loginRechToday = !_jedis.exists(mloginRechListKey) ? 0L : _jedis.scard(mloginRechListKey);
        Long avg_logintimes_rech = 0L==loginRechToday ? 0L : (mlogintimesRech / loginRechToday);




        //flush to mlogin daily list
        String mloginDailyListKey = "mlogin:" + game_abbr + ":" + todayDate + ":record";
        String mloginDailyListValue = login_datetime + ":" + client + ":" + platform_id + ":" + server_id +
                ":" + appver + ":" + uname + ":" + system  + ":" + model + ":" + resolution + ":" + sp + ":" + network +
                ":" + client_ip + ":" + district + ":" + osver + ":" + osbuilder + ":" + devtype + ":" +
                ifnew.toString() + ":" + (ifRech?"1":"0");
        _jedis.rpush(mloginDailyListKey, mloginDailyListValue);





        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)
                && _dbFlushTimer.ifItsTime2FlushDb(client.toString()+platform_id+server_id+appver)) {
            boolean sql_ret = false;
            String sqls = "";

            String inssql_overalldata = String.format("INSERT INTO overalldata (client, platform, accounts)" +
                            " VALUES (%d, '%s', %d) ON DUPLICATE KEY UPDATE accounts=%d;", client, platform_id,
                    overalldata_accounts, overalldata_accounts);

            String inssql_overalldatadaily = String.format("INSERT INTO overalldatadaily (client, platform, date, dau," +
                            "daunew) VALUES (%d, '%s', %d, %d, %d) ON DUPLICATE KEY UPDATE dau=%d, daunew=%d;", client,
                    platform_id, todayDate, overalldatadaily_dau, overalldatadaily_daunew, overalldatadaily_dau,
                    overalldatadaily_daunew);

            String overalldatahourly_tb = "overalldatahourly_" + todayStr;
            String dmlsql_overalldatahourly_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`hour` tinyint(3) unsigned NOT NULL," +
                    "`hau` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`maxonline` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launchdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `hour`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", overalldatahourly_tb);
            String inssql_overalldatahourly = String.format("INSERT INTO %s (client, platform, hour, hau) VALUES (" +
                    "%d, '%s', '%s', %d) ON DUPLICATE KEY UPDATE hau=%d;", overalldatahourly_tb, client, platform_id,
                    curHour, overalldatahourly_hau, overalldatahourly_hau);

            String inssql_overalldatadailyverly = String.format("INSERT INTO overalldatadailyverly (client, platform, " +
                            "date, version, dau, daunew) VALUES (%d, '%s', %d, '%s', %d, %d) ON DUPLICATE KEY UPDATE " +
                            "dau=%d, daunew=%d;", client, platform_id, todayDate, appver, overalldatadailyverly_dau,
                    overalldatadailyverly_daunew, overalldatadailyverly_dau, overalldatadailyverly_daunew);

            String inssql_signinlogindaily = String.format("INSERT INTO signinlogindaily (client, platform, server, date," +
                            "version, newacc, logins) VALUES (%d, '%s', '%s', %d, '%s', %d, %d) ON DUPLICATE KEY UPDATE" +
                            " newacc=%d, logins=%d;", client, platform_id, server_id, todayDate, appver,
                    signinlogindaily_newacc, signinlogindaily_logins, signinlogindaily_newacc, signinlogindaily_logins);

            String signinloginhourly_tb = "signinloginhourly_" + todayStr;
            String dmlsql_signinloginhourly_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`hour` tinyint(3) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`newdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`newacc` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`logins` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launchdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`launch` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`lasttime` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `server`, `hour`, `version`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", signinloginhourly_tb);
            String inssql_signinloginhourly = String.format("INSERT INTO %s (client, platform, server, hour, version," +
                            "newacc, logins) VALUES (%d, '%s', '%s', '%s', '%s', %d, %d) ON DUPLICATE KEY UPDATE " +
                            "newacc=%d, logins=%d;", signinloginhourly_tb, client, platform_id, server_id, curHour,
                    appver, signinloginhourly_newacc, signinloginhourly_logins, signinloginhourly_newacc,
                    signinloginhourly_logins);

            String loginlist_tb = "loginlist_" + todayStr;
            String dmlsql_loginlist_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`account` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`devid` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`os` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`appver` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`model` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`resolution` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`operator` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`network` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`clientip` char(32) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`district` char(128) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`osver` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`osbuilder` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`devtype` char(64) CHARACTER SET UTF8 NOT NULL DEFAULT ''," +
                    "`ifnew` tinyint(2) unsigned NOT NULL DEFAULT 0," +
                    "`ifrecharge` tinyint(2) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", loginlist_tb);
            String inssql_loginlist = String.format("INSERT INTO %s (client, platform, server, version, account, " +
                            "devid, os, appver, model, resolution, operator, network, clientip, district, osver," +
                            "osbuilder, devtype, ifnew, ifrecharge) VALUES (%d, '%s', '%s', '%s', '%s', '%s', '%s', " +
                            "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d);", loginlist_tb,
                    client, platform_id, server_id, appver, uname, devid, system, appver, model, resolution, sp,
                    network, client_ip, district, osver, osbuilder, devtype, ifnew, ifRech?1:0);

            String newonlinert_tb = "newonlinert_" + todayStr;
            String dmlsql_newonlinert_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`datetime` int(11) unsigned NOT NULL," +
                    "`version` char(10) CHARACTER SET UTF8 NOT NULL," +
                    "`online` int(10) unsigned NOT NULL DEFAULT 0," +
                    "`newdev` int(11) unsigned NOT NULL DEFAULT 0," +
                    "`newacc` int(11) unsigned NOT NULL DEFAULT 0," +
                    "PRIMARY KEY (`id`)," +
                    "UNIQUE KEY `platform` (`client`, `platform`, `server`, `datetime`, `version`)" +
                    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;", newonlinert_tb);
            String inssql_newonlinert = String.format("INSERT INTO %s (client, platform, server, datetime, version, newacc)" +
                            " VALUES (%d, '%s', '%s', %d, '%s', %d) ON DUPLICATE KEY UPDATE newacc=%d;", newonlinert_tb,
                    client, platform_id, server_id, todayMinuteDate, appver, newonlinert_newacc, newonlinert_newacc);


            String activedaily_hau_colname = "daut" + curHour;
            String inssql_activedaily = String.format("INSERT INTO activedaily (client, platform, server, date, version," +
                    "dau, daunew, %s) VALUES (%d, '%s', '%s', %d, '%s', %d, %d, %d) ON DUPLICATE KEY UPDATE %s=%d;",
                    activedaily_hau_colname, client, platform_id, server_id, todayDate, appver, signinlogindaily_logins,
                    signinlogindaily_newacc, signinloginhourly_logins, activedaily_hau_colname, signinloginhourly_logins);

            String inssql_lostretdaily = String.format("INSERT INTO lostretdaily (client, platform, server, date, version," +
                    "retd7_dau, retd7_rechargers, retd14_dau, retd14_rechargers, retd30_dau, retd30_rechargers) VALUES (" +
                    "%d, '%s', '%s', %d, '%s', %d, %d, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE retd7_dau=%d, " +
                    "retd7_rechargers=%d, retd14_dau=%d, retd14_rechargers=%d, retd30_dau=%d, retd30_rechargers=%d;",
                    client, platform_id, server_id, todayDate, appver, retListd7Dau, retListd7Rech, retListd14Dau,
                    retListd14Rech, retListd30Dau, retListd30Rech, retListd7Dau, retListd7Rech, retListd14Dau,
                    retListd14Rech, retListd30Dau, retListd30Rech);

            String inssql_su_participation_daily = String.format("INSERT INTO su_participation_daily (client, platform," +
                            "server, date, version, avg_logintimes_newacc, avg_logintimes_dau, avg_logintimes_rechargers) VALUES (%d," +
                            "'%s', '%s', %d, '%s', %d, %d, %d) ON DUPLICATE KEY UPDATE avg_logintimes_newacc=%d, " +
                            "avg_logintimes_dau=%d, avg_logintimes_rechargers=%d;", client, platform_id, server_id,
                    login_datetime_int, appver, avg_logintimes_newacc, avg_logintimes_dau, avg_logintimes_rech,
                    avg_logintimes_newacc, avg_logintimes_dau, avg_logintimes_rech);

            String inssql_su_participation_dist_daily = "";
            String perNewaccLogintimesDistKey = "";
            String perDauLogintimesDistKey = "";
            String perRechLogintimesDistKey = "";

            for (Integer i=1 ; i<=7 ; i++) {
                inssql_su_participation_dist_daily = "INSERT INTO su_participation_dist_daily (client," +
                        "platform, server, date, version, distinterval";
                String su_participation_dist_daily_col = "";
                String su_participation_dist_daily_prepare_col = "";
                String su_participation_dist_daily_duplicate_col = "";
                boolean ifIns = false;

                if (1 == ifnew) {
                    su_participation_dist_daily_col += ", newacc_logintimes_perday";

                    perNewaccLogintimesDistKey = perNewaccLogintimesDistFormerKey + i.toString() + perNewaccLogintimesDistLatterKey;
                    Long perNewaccLogintimes = !_jedis.exists(perNewaccLogintimesDistKey) ? 0L : _jedis.scard(perNewaccLogintimesDistKey);

                    su_participation_dist_daily_prepare_col += ", " + perNewaccLogintimes.toString();
                    su_participation_dist_daily_duplicate_col += "newacc_logintimes_perday = " + perNewaccLogintimes.toString();

                    ifIns = true;
                }
                if (ifDau) {
                    su_participation_dist_daily_col += ", dau_logintimes_perday";

                    perDauLogintimesDistKey = perDauLogintimesDistFormerKey + i.toString() + perDauLogintimesDistLatterKey;
                    Long perDauLogintimes = !_jedis.exists(perDauLogintimesDistKey) ? 0L : _jedis.scard(perDauLogintimesDistKey);

                    su_participation_dist_daily_prepare_col += ", " + perDauLogintimes.toString();
                    if (!"".equals(su_participation_dist_daily_duplicate_col)) {
                        su_participation_dist_daily_duplicate_col += ", ";
                    }
                    su_participation_dist_daily_duplicate_col += "dau_logintimes_perday = " + perDauLogintimes.toString();

                    ifIns = true;
                }
                if (ifRech) {
                    su_participation_dist_daily_col += ", rechargers_logintimes_perday";

                    perRechLogintimesDistKey = perRechLogintimesDistFormerKey + i.toString() + perRechLogintimesDistLatterKey;
                    Long perRechLogintimes = !_jedis.exists(perRechLogintimesDistKey) ? 0L : _jedis.scard(perRechLogintimesDistKey);

                    su_participation_dist_daily_prepare_col += ", " + perRechLogintimes.toString();
                    if (!"".equals(su_participation_dist_daily_duplicate_col)) {
                        su_participation_dist_daily_duplicate_col += ", ";
                    }
                    su_participation_dist_daily_duplicate_col += "rechargers_logintimes_perday = " + perRechLogintimes.toString();

                    ifIns = true;
                }

                if (ifIns) {
                    inssql_su_participation_dist_daily += (su_participation_dist_daily_col + ") VALUES ("+client+", "+
                        platform_id+", "+server_id+", "+todayDate+", '"+appver+"', "+i.toString()+
                        su_participation_dist_daily_prepare_col+") ON DUPLICATE KEY UPDATE "+
                        su_participation_dist_daily_duplicate_col+";");

                    sqls += inssql_su_participation_dist_daily;
                }
            }

//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldata);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldatadaily);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_overalldatadailyverly);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_signinlogindaily);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_signinloginhourly_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_signinloginhourly);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_loginlist_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_loginlist);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, dmlsql_newonlinert_tb);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            try {
//                sql_ret = _dbconnect.DirectUpdate(game_abbr, inssql_newonlinert);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }

            sqls += inssql_overalldata;
            sqls += inssql_overalldatadaily;
            sqls += dmlsql_overalldatahourly_tb;
            sqls += inssql_overalldatahourly;
            sqls += inssql_overalldatadailyverly;
            sqls += inssql_signinlogindaily;
            sqls += dmlsql_signinloginhourly_tb;
            sqls += inssql_signinloginhourly;
            sqls += dmlsql_loginlist_tb;
            sqls += inssql_loginlist;
            sqls += dmlsql_newonlinert_tb;
            sqls += inssql_newonlinert;
            sqls += DailyremainSql;
            sqls += inssql_activedaily;
            sqls += inssql_mactivedaily;
            sqls += inssql_mlostretdaily;
            sqls += inssql_lostretdaily;
            sqls += inssql_su_participation_daily;

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
