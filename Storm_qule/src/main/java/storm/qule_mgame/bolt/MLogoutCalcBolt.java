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
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/11/30.
 */
public class MLogoutCalcBolt extends BaseBasicBolt {
    private Jedis _jedis;
    private static Properties _prop = new Properties();
    private static mysql _dbconnect = null;

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

        _dbconnect = new mysql();

    }

    @Override
    public void execute(Tuple tuple , BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        String game_abbr = tuple.getStringByField("game_abbr");
        String platform_id = tuple.getStringByField("platform_id");
        String server_id = tuple.getStringByField("server_id");
        String logout_datetime = tuple.getStringByField("logout_datetime");
        String devid = tuple.getStringByField("devid");
        String uname = tuple.getStringByField("uname");
        String cname = tuple.getStringByField("cname");
        String oltime = tuple.getStringByField("oltime");

        Long logout_datetime_int = Long.parseLong(logout_datetime);
        String todayStr = date.timestamp2str(logout_datetime_int, "yyyyMMdd");
        String todayHourStr = date.timestamp2str(logout_datetime_int, "yyyyMMdd-HH");
        String curHour = date.timestamp2str(logout_datetime_int, "H");

        Long cur_time_ts = System.currentTimeMillis() / 1000;

        Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

        Integer oltime_int = Integer.parseInt(oltime);

        //根据用户登录列表信息获取用户系统、版本号信息
        String system = "";
        String appver = "";
//        String model = "";
//        String resolution = "";
//        String operator = "";
//        String network = "";
//        String clientip = "";
//        String district = "";
//        String osver = "";
//        String osbuilder = "";
//        String devtype = "";

        String mloginListKey = "mlogin:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";
        List mloginListLatest = _jedis.lrange(mloginListKey, -1, -1);       //获取最近登陆信息
        if (!mloginListLatest.isEmpty()) {
            String mloginLatestDetailKey = mloginListLatest.get(0).toString();
            List<String> mloginLatestDetail = _jedis.hmget(mloginLatestDetailKey, "os", "appver"/*, "devid", "model", "resolution",
                    "operator", "network", "clientip", "district", "osver", "osbuilder", "devtype"*/);
            if (!mloginLatestDetail.isEmpty()) {
                system = mloginLatestDetail.get(0);
                appver = mloginLatestDetail.get(1);
                //devid = mloginLatestDetail.get(2);
//                model = mloginLatestDetail.get(3);
//                resolution = mloginLatestDetail.get(4);
//                operator = mloginLatestDetail.get(5);
//                network = mloginLatestDetail.get(6);
//                clientip = mloginLatestDetail.get(7);
//                district = mloginLatestDetail.get(8);
//                osver = mloginLatestDetail.get(9);
//                osbuilder = mloginLatestDetail.get(10);
//                devtype = mloginLatestDetail.get(11);
            }
        }

        Integer client = system2client.turnSystem2ClientId(system);


        //当天使用总时长
        String moltimeAccKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":oltime:acc:incr";

        Long moltimeAcc = _jedis.incrBy(moltimeAccKey, oltime_int);

        //当天各时段使用总时长
        String moltimeHourlyAccKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayHourStr + ":" + appver + ":oltime:acc:incr";

        Long moltimeHourlyAcc = _jedis.incrBy(moltimeHourlyAccKey, oltime_int);


        String moltimeNewaccKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":oltime:newacc:incr";
        String moltimeDauKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":oltime:dau:incr";
        String moltimeRechKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":oltime:rechargers:incr";

        Long moltimeNewacc = !_jedis.exists(moltimeNewaccKey) ? 0L : Integer.parseInt(_jedis.get(moltimeNewaccKey));
        Long moltimeDau = !_jedis.exists(moltimeDauKey) ? 0L : Integer.parseInt(_jedis.get(moltimeDauKey));
        Long moltimeRech = !_jedis.exists(moltimeRechKey) ? 0L : Integer.parseInt(_jedis.get(moltimeRechKey));

        boolean ifNew = false, ifDau = false, ifRech = false;


        //是否新用户(对应天新用户)
        Long thisday_ts = cur_time_ts - oltime_int;
        String thisdayStr = date.timestamp2str(thisday_ts, "yyyyMMdd");
        String loginAccListSvrNewThisdayKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + thisdayStr + ":" + appver + ":account:new:set";

        String perNewaccOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_perday:";
        String perNewaccOltimeDistLatterKey = ":newacc:set";

        String pertimeNewaccOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_pertime:";
        String pertimeNewaccOltimeDistLatterKey = ":newacc:set";

        if (_jedis.sismember(loginAccListSvrNewThisdayKey, uname)) {
            //新用户对应单日游戏时长
            String perNewaccOltimeKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":newacc_lasttime_perday:incr";
            Long thisNewAccOltime =  _jedis.incrBy(perNewaccOltimeKey, oltime_int);

            String perNewaccOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                perNewaccOltimeDistKey = perNewaccOltimeDistFormerKey + i.toString() + perNewaccOltimeDistLatterKey;
                if (1 == _jedis.srem(perNewaccOltimeDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisNewAccOltime <= 10) {
                segmentid = 1;
            } else if (thisNewAccOltime > 10 && thisNewAccOltime <= 60) {
                segmentid = 2;
            } else if (thisNewAccOltime > 60 && thisNewAccOltime <= 180) {
                segmentid = 3;
            } else if (thisNewAccOltime > 180 && thisNewAccOltime <= 600) {
                segmentid = 4;
            } else if (thisNewAccOltime > 600 && thisNewAccOltime <= 1800) {
                segmentid = 5;
            } else if (thisNewAccOltime > 1800 && thisNewAccOltime <= 3600) {
                segmentid = 6;
            } else if (thisNewAccOltime > 3600 && thisNewAccOltime <= 7200) {
                segmentid = 7;
            } else if (thisNewAccOltime > 7200 && thisNewAccOltime <= 14400) {
                segmentid = 8;
            } else if (thisNewAccOltime > 14400) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(perNewaccOltimeDistFormerKey + segmentid.toString() + perNewaccOltimeDistLatterKey, uname);
            }


            //新用户对应单次游戏时长
            String mlogintimesNewaccKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":logintimes:newacc:incr";
            Long mlogintimesNewacc = !_jedis.exists(mlogintimesNewaccKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesNewaccKey));

            Long thisNewAccOltimePertime = 0L==mlogintimesNewacc ? 0L : (thisNewAccOltime / mlogintimesNewacc);

            String pertimeNewaccOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                pertimeNewaccOltimeDistKey = pertimeNewaccOltimeDistFormerKey + i.toString() + pertimeNewaccOltimeDistLatterKey;
                if (1 == _jedis.srem(pertimeNewaccOltimeDistKey, uname)) {
                    break;
                }
            }

            segmentid = 0;
            if (thisNewAccOltimePertime <= 3) {
                segmentid = 1;
            } else if (thisNewAccOltimePertime > 3 && thisNewAccOltimePertime <= 10) {
                segmentid = 2;
            } else if (thisNewAccOltimePertime > 10 && thisNewAccOltimePertime <= 30) {
                segmentid = 3;
            } else if (thisNewAccOltimePertime > 30 && thisNewAccOltimePertime <= 60) {
                segmentid = 4;
            } else if (thisNewAccOltimePertime > 60 && thisNewAccOltimePertime <= 180) {
                segmentid = 5;
            } else if (thisNewAccOltimePertime > 180 && thisNewAccOltimePertime <= 600) {
                segmentid = 6;
            } else if (thisNewAccOltimePertime > 600 && thisNewAccOltimePertime <= 1800) {
                segmentid = 7;
            } else if (thisNewAccOltimePertime > 1800 && thisNewAccOltimePertime <= 3600) {
                segmentid = 8;
            } else if (thisNewAccOltimePertime > 3600) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(pertimeNewaccOltimeDistFormerKey + segmentid.toString() + pertimeNewaccOltimeDistLatterKey, uname);
            }



            moltimeNewacc = _jedis.incrBy(moltimeNewaccKey, oltime_int);
            ifNew = true;
        }

        String loginAccListSvrNewTodayKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":account:new:set";
        Long loginAccNewToday = !_jedis.exists(loginAccListSvrNewTodayKey) ? 0L : _jedis.scard(loginAccListSvrNewTodayKey);
        Long avg_lasttime_newacc = 0L==loginAccNewToday ? 0L : (moltimeNewacc / loginAccNewToday);


        //是否活跃用户
        String loginAccListSvrKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":account:set";

        String perDauOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_perday:";
        String perDauOltimeDistLatterKey = ":dau:set";

        String pertimeDauOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_pertime:";
        String pertimeDauOltimeDistLatterKey = ":dau:set";

        if (_jedis.sismember(loginAccListSvrKey, uname)) {
            //活跃用户对应单日游戏时长
            String perDauOltimeKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":dau_lasttime_perday:incr";
            Long thisDauOltime =  _jedis.incrBy(perDauOltimeKey, oltime_int);

            String perDauOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                perDauOltimeDistKey = perDauOltimeDistFormerKey + i.toString() + perDauOltimeDistLatterKey;
                if (1 == _jedis.srem(perDauOltimeDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisDauOltime <= 10) {
                segmentid = 1;
            } else if (thisDauOltime > 10 && thisDauOltime <= 60) {
                segmentid = 2;
            } else if (thisDauOltime > 60 && thisDauOltime <= 180) {
                segmentid = 3;
            } else if (thisDauOltime > 180 && thisDauOltime <= 600) {
                segmentid = 4;
            } else if (thisDauOltime > 600 && thisDauOltime <= 1800) {
                segmentid = 5;
            } else if (thisDauOltime > 1800 && thisDauOltime <= 3600) {
                segmentid = 6;
            } else if (thisDauOltime > 3600 && thisDauOltime <= 7200) {
                segmentid = 7;
            } else if (thisDauOltime > 7200 && thisDauOltime <= 14400) {
                segmentid = 8;
            } else if (thisDauOltime > 14400) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(perDauOltimeDistFormerKey + segmentid.toString() + perDauOltimeDistLatterKey, uname);
            }


            //活跃用户对应单次游戏时长
            String mlogintimesDauKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":logintimes:dau:incr";
            Long mlogintimesDau = !_jedis.exists(mlogintimesDauKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesDauKey));

            Long thisDauOltimePertime = 0L==mlogintimesDau ? 0L : (thisDauOltime / mlogintimesDau);

            String pertimeDauOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                pertimeDauOltimeDistKey = pertimeDauOltimeDistFormerKey + i.toString() + pertimeDauOltimeDistLatterKey;
                if (1 == _jedis.srem(pertimeDauOltimeDistKey, uname)) {
                    break;
                }
            }

            segmentid = 0;
            if (thisDauOltimePertime <= 3) {
                segmentid = 1;
            } else if (thisDauOltimePertime > 3 && thisDauOltimePertime <= 10) {
                segmentid = 2;
            } else if (thisDauOltimePertime > 10 && thisDauOltimePertime <= 30) {
                segmentid = 3;
            } else if (thisDauOltimePertime > 30 && thisDauOltimePertime <= 60) {
                segmentid = 4;
            } else if (thisDauOltimePertime > 60 && thisDauOltimePertime <= 180) {
                segmentid = 5;
            } else if (thisDauOltimePertime > 180 && thisDauOltimePertime <= 600) {
                segmentid = 6;
            } else if (thisDauOltimePertime > 600 && thisDauOltimePertime <= 1800) {
                segmentid = 7;
            } else if (thisDauOltimePertime > 1800 && thisDauOltimePertime <= 3600) {
                segmentid = 8;
            } else if (thisDauOltimePertime > 3600) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(pertimeDauOltimeDistFormerKey + segmentid.toString() + pertimeDauOltimeDistLatterKey, uname);
            }


            moltimeDau = _jedis.incrBy(moltimeDauKey, oltime_int);
            ifDau = true;
        }

        String loginAccListSvrDailyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":account:set";
        Long loginAccToday = !_jedis.exists(loginAccListSvrDailyKey) ? 0L : _jedis.scard(loginAccListSvrDailyKey);
        Long avg_lasttime_dau = 0L==loginAccToday ? 0L : (moltimeDau / loginAccToday);


        //是否充值用户
        String mrechargeListKey = "mrecharge:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + uname + ":record";

        String perRechargerOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_perday:";
        String perRechargerOltimeDistLatterKey = ":rechargers:set";

        String pertimeRechOltimeDistFormerKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                todayStr + ":" + appver + ":lasttime_pertime:";
        String pertimeRechOltimeDistLatterKey = ":rechargers:set";

        if (_jedis.exists(mrechargeListKey)) {
            //充值用户对应单日游戏时长
            String perRechOltimeKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":" + uname + ":rechargers_lasttime_perday:incr";
            Long thisRechargerOltime =  _jedis.incrBy(perRechOltimeKey, oltime_int);

            String perRechargerOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                perRechargerOltimeDistKey = perRechargerOltimeDistFormerKey + i.toString() + perRechargerOltimeDistLatterKey;
                if (1 == _jedis.srem(perRechargerOltimeDistKey, uname)) {
                    break;
                }
            }

            Integer segmentid = 0;
            if (thisRechargerOltime <= 10) {
                segmentid = 1;
            } else if (thisRechargerOltime > 10 && thisRechargerOltime <= 60) {
                segmentid = 2;
            } else if (thisRechargerOltime > 60 && thisRechargerOltime <= 180) {
                segmentid = 3;
            } else if (thisRechargerOltime > 180 && thisRechargerOltime <= 600) {
                segmentid = 4;
            } else if (thisRechargerOltime > 600 && thisRechargerOltime <= 1800) {
                segmentid = 5;
            } else if (thisRechargerOltime > 1800 && thisRechargerOltime <= 3600) {
                segmentid = 6;
            } else if (thisRechargerOltime > 3600 && thisRechargerOltime <= 7200) {
                segmentid = 7;
            } else if (thisRechargerOltime > 7200 && thisRechargerOltime <= 14400) {
                segmentid = 8;
            } else if (thisRechargerOltime > 14400) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(perRechargerOltimeDistFormerKey + segmentid.toString() + perRechargerOltimeDistLatterKey, uname);
            }


            //充值用户对应单次游戏时长
            String mlogintimesRechKey = "moltime:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id + ":" +
                    todayStr + ":" + appver + ":logintimes:rechargers:incr";
            Long mlogintimesRech = !_jedis.exists(mlogintimesRechKey) ? 0L : Integer.parseInt(_jedis.get(mlogintimesRechKey));

            Long thisRechOltimePertime = 0L==mlogintimesRech ? 0L : (thisRechargerOltime / mlogintimesRech);

            String pertimeRechOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                pertimeRechOltimeDistKey = pertimeRechOltimeDistFormerKey + i.toString() + pertimeRechOltimeDistLatterKey;
                if (1 == _jedis.srem(pertimeRechOltimeDistKey, uname)) {
                    break;
                }
            }

            segmentid = 0;
            if (thisRechOltimePertime <= 3) {
                segmentid = 1;
            } else if (thisRechOltimePertime > 3 && thisRechOltimePertime <= 10) {
                segmentid = 2;
            } else if (thisRechOltimePertime > 10 && thisRechOltimePertime <= 30) {
                segmentid = 3;
            } else if (thisRechOltimePertime > 30 && thisRechOltimePertime <= 60) {
                segmentid = 4;
            } else if (thisRechOltimePertime > 60 && thisRechOltimePertime <= 180) {
                segmentid = 5;
            } else if (thisRechOltimePertime > 180 && thisRechOltimePertime <= 600) {
                segmentid = 6;
            } else if (thisRechOltimePertime > 600 && thisRechOltimePertime <= 1800) {
                segmentid = 7;
            } else if (thisRechOltimePertime > 1800 && thisRechOltimePertime <= 3600) {
                segmentid = 8;
            } else if (thisRechOltimePertime > 3600) {
                segmentid = 9;
            }
            if (0 != segmentid) {
                _jedis.sadd(pertimeRechOltimeDistFormerKey + segmentid.toString() + pertimeRechOltimeDistLatterKey, uname);
            }


            moltimeRech = _jedis.incrBy(moltimeRechKey, oltime_int);
            ifRech = true;
        }

        String loginRechListDailyKey = "mlogin:" + game_abbr + ":" + system + ":" + platform_id + ":" + server_id +
                ":" + todayStr + ":" + appver + ":rechargers:set";
        Long loginRechToday = !_jedis.exists(loginRechListDailyKey) ? 0L : _jedis.scard(loginRechListDailyKey);
        Long avg_lasttime_rechargers = 0L==loginRechToday ? 0L : (moltimeRech / loginRechToday);



        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");

        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;
            String sqls = "";

            String inssql_su_participation_daily = String.format("INSERT INTO su_participation_daily (client, platform," +
                    "server, date, version, avg_lasttime_newacc, avg_lasttime_dau, avg_lasttime_rechargers) VALUES (%d," +
                    "%s, %s, %d, %s, %d, %d, %d) ON DUPLICATE KEY UPDATE avg_lasttime_newacc=%d, avg_lasttime_dau=%d," +
                    "avg_lasttime_rechargers=%d;", client, platform_id, server_id, todayDate, appver,
                    avg_lasttime_newacc, avg_lasttime_dau, avg_lasttime_rechargers, avg_lasttime_newacc,
                    avg_lasttime_dau, avg_lasttime_rechargers);


            String inssql_su_participation_dist_daily = "";

            String perdayNewaccOltimeDistKey = "";
            String pertimeNewaccOltimeDistKey = "";
            String perdayDauOltimeDistKey = "";
            String pertimeDauOltimeDistKey = "";
            String perdayRechOltimeDistKey = "";
            String pertimeRechOltimeDistKey = "";
            for (Integer i=1 ; i<=9 ; i++) {
                inssql_su_participation_dist_daily = "INSERT INTO su_participation_dist_daily (client," +
                        "platform, server, date, version, distinterval";
                String su_participation_dist_daily_col = "";
                String su_participation_dist_daily_prepare_col = "";
                String su_participation_dist_daily_duplicate_col = "";
                boolean ifIns = false;

                if (ifNew) {
                    su_participation_dist_daily_col += ", newacc_lasttime_perday, newacc_lasttime_pertime";

                    pertimeNewaccOltimeDistKey = pertimeNewaccOltimeDistFormerKey + i.toString() + pertimeNewaccOltimeDistLatterKey;
                    Long pertimeNewaccOltime = !_jedis.exists(pertimeNewaccOltimeDistKey) ? 0L : _jedis.scard(pertimeNewaccOltimeDistKey);

                    perdayNewaccOltimeDistKey = perNewaccOltimeDistFormerKey + i.toString() + perNewaccOltimeDistLatterKey;
                    Long perdayNewaccOltime = !_jedis.exists(perdayNewaccOltimeDistKey) ? 0L : _jedis.scard(perdayNewaccOltimeDistKey);

                    su_participation_dist_daily_prepare_col += ", "+perdayNewaccOltime.toString()+", "+pertimeNewaccOltime.toString();
                    su_participation_dist_daily_duplicate_col += "newacc_lasttime_perday = " + perdayNewaccOltime.toString() +
                            ", newacc_lasttime_pertime = " + pertimeNewaccOltime.toString();

                    ifIns = true;
                }
                if (ifDau) {
                    su_participation_dist_daily_col += ", dau_lasttime_perday, dau_lasttime_pertime";

                    pertimeDauOltimeDistKey = pertimeDauOltimeDistFormerKey + i.toString() + pertimeDauOltimeDistLatterKey;
                    Long pertimeDauOltime = !_jedis.exists(pertimeDauOltimeDistKey) ? 0L : _jedis.scard(pertimeDauOltimeDistKey);

                    perdayDauOltimeDistKey = perDauOltimeDistFormerKey + i.toString() + perDauOltimeDistLatterKey;
                    Long perdayDauOltime = !_jedis.exists(perdayDauOltimeDistKey) ? 0L : _jedis.scard(perdayDauOltimeDistKey);

                    su_participation_dist_daily_prepare_col += ", " + perdayDauOltime.toString() + ", " + pertimeDauOltime.toString();
                    su_participation_dist_daily_duplicate_col += ", dau_lasttime_perday = " + perdayDauOltime.toString() +
                            ", dau_lasttime_pertime = " + pertimeDauOltime.toString();

                    ifIns = true;
                }
                if (ifRech) {
                    su_participation_dist_daily_col += ", rechargers_lasttime_perday, rechargers_lasttime_pertime";

                    pertimeRechOltimeDistKey = pertimeRechOltimeDistFormerKey + i.toString() + pertimeRechOltimeDistLatterKey;
                    Long pertimeRechOltime = !_jedis.exists(pertimeRechOltimeDistKey) ? 0L : _jedis.scard(pertimeRechOltimeDistKey);

                    perdayRechOltimeDistKey = perRechargerOltimeDistFormerKey + i.toString() + perRechargerOltimeDistLatterKey;
                    Long perdayRechOltime = !_jedis.exists(perdayRechOltimeDistKey) ? 0L : _jedis.scard(perdayRechOltimeDistKey);

                    su_participation_dist_daily_prepare_col += ", " + perdayRechOltime.toString() + ", " + pertimeRechOltime.toString();
                    su_participation_dist_daily_duplicate_col += ", rechargers_lasttime_perday = " + perdayRechOltime.toString() +
                            ", rechargers_lasttime_pertime = " + pertimeRechOltime.toString();

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

            String inssql_signinlogindaily = String.format("INSERT INTO signinlogindaily (client, platform, server, " +
                    "date, version, lasttime) VALUES (%d, %s, %s, %d, %s, %d) ON DUPLICATE KEY UPDATE lasttime=%d;",
                    client, platform_id, server_id, todayDate, appver, moltimeAcc, moltimeAcc);
            String signinloginhourly_tb = "signinloginhourly_" + todayStr;
            String dmlsql_signinloginhourly_tb = String.format("CREATE TABLE IF NOT EXISTS %s (" +
                    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT," +
                    "`client` tinyint(3) unsigned NOT NULL," +
                    "`platform` mediumint(5) unsigned NOT NULL," +
                    "`server` mediumint(5) unsigned NOT NULL," +
                    "`hour` tiny(3) unsigned NOT NULL," +
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
                    "lasttime) VALUES (%d, %s, %s, %s, %s, %d) ON DUPLICATE KEY UPDATE lasttime=%d;",
                    signinloginhourly_tb, client, platform_id, server_id, curHour, appver, moltimeHourlyAcc,
                    moltimeHourlyAcc);

            sqls += inssql_su_participation_daily;
            sqls += inssql_signinlogindaily;
            sqls += dmlsql_signinloginhourly_tb;
            sqls += inssql_signinloginhourly;

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