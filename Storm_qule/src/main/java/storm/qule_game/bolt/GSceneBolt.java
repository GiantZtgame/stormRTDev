package storm.qule_game.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import redis.clients.jedis.Jedis;
import storm.qule_util.*;

import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/8/19.
 */
public class GSceneBolt extends BaseBasicBolt {
    private static final String LOG_REG_RES_LOADED_SIGN = "reg_res_loaded";
    private static final String LOG_SCENE_START_LOADING_SIGN = "scene_start_loading";
    private static final String LOG_ENTER_SCENE_SIGN = "enter_scene";

    private Jedis _jedis;
    private static Properties _prop = new Properties();

    private static timerCfgLoader _gamecfgLoader = new timerCfgLoader();
    private static cfgLoader _cfgLoader = new cfgLoader();
    private static String _gamecfg;

    private static mysql _dbconnect = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("+++++++ GScene verify prepare!!!");
        String conf;
        if (stormConf.get("isOnline").toString().equals("true")) {
            //conf = "/config/games.properties";
            _gamecfg = stormConf.get("gamecfg_path").toString();
        }
        else {
            _gamecfg = "/config/test.games.properties";
        }

        _prop = _cfgLoader.loadConfig(_gamecfg, Boolean.parseBoolean(stormConf.get("isOnline").toString()));

        _jedis = new jedisUtil().getJedis(_prop.getProperty("redis.host"), Integer.parseInt(_prop.getProperty("redis.port")));

        _dbconnect = new mysql();
//        InputStream game_cfg_in = getClass().getResourceAsStream(_gamecfg);
//        try {
//            _prop.load(game_cfg_in);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        //refresh gamecfg
        _prop = _gamecfgLoader.loadCfg(_gamecfg, _prop);

        boolean ifRegResLoadedSql = false;
        boolean ifSceneStartLoadingSql = false;
        boolean ifEnterSceneSql = false;

        String sentence = tuple.getString(0);
        String[] logs;
        logs = sentence.split("\\|");

        if (logs.length > 5) {
            String game_abbr = logs[0];
            String platform_id = logs[1];
            String server_id = logs[2];
            String token = logs[3];
            String datetime_str = logs[4];
            String log_type = logs[5];
            Long datetime = date.str2timestamp(datetime_str);

            String todayStr = date.timestamp2str(datetime, "yyyyMMdd");
            Long todayDate = date.str2timestamp(todayStr, "yyyyMMdd");

            //资源加载完成，进入建角页面
            String regResLoadedKey = "regResLoaded:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":incr";
            Integer regResLoaded = !_jedis.exists(regResLoadedKey) ? 0 : Integer.parseInt(_jedis.get(regResLoadedKey));
            //开始加载场景
            String sceneStartLoadingKey = "sceneStartLoading:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":incr";
            Integer sceneStartLoading = !_jedis.exists(sceneStartLoadingKey) ? 0 : Integer.parseInt(_jedis.get(sceneStartLoadingKey));
            //场景加载完成，玩家进入场景
            String enterSceneKey = "enterScene:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":incr";
            Integer enterScene = !_jedis.exists(enterSceneKey) ? 0 : Integer.parseInt(_jedis.get(enterSceneKey));

            String log_key = _prop.getProperty("game." + game_abbr + ".key");
            String raw_str = game_abbr + platform_id + server_id + log_key;
            String token_gen = "";
            try {
                token_gen = new md5().gen_md5(raw_str);
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }

            if (7 == logs.length) {
                String uname = logs[6];

                if (token_gen.equals(token) && log_type.equals(LOG_REG_RES_LOADED_SIGN)) {
                    String regResLoadedListKey = "regResLoaded:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":set";

                    if (!_jedis.sismember(regResLoadedListKey, uname)) {
                        regResLoaded++;
                        _jedis.sadd(regResLoadedListKey, uname);
                        _jedis.expire(regResLoadedListKey, 30 * 24 * 3600);
                        _jedis.set(regResLoadedKey, regResLoaded.toString());
                        _jedis.expire(regResLoadedKey, 24 * 3600);
                        ifRegResLoadedSql = true;
                    }
                }
            } else if (9 == logs.length) {
                String uname = logs[6];
                String cname = logs[7];
                String level = logs[8];

                if (token_gen.equals(token) && log_type.equals(LOG_SCENE_START_LOADING_SIGN)) {
                    Utils.sleep(100);

                    String sceneStartLoadingListKey = "sceneStartLoading:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":set";

                    if (!_jedis.sismember(sceneStartLoadingListKey, uname)) {
                        //判断是否为新用户
                        String regkey = "greginfo-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname;
                        if (_jedis.exists(regkey)) {
                            List userinfo = _jedis.lrange(regkey, 0, 0);
                            String[] userinfoArr = userinfo.get(0).toString().split("-");
                            if (userinfoArr.length == 6) {
                                String userRegDateStr = date.timestamp2str(Long.parseLong(userinfoArr[5]), "yyyyMMdd");
                                if (userRegDateStr.equals(todayStr)) {
                                    sceneStartLoading++;
                                    _jedis.sadd(sceneStartLoadingListKey, uname);
                                    _jedis.expire(sceneStartLoadingListKey, 30 * 24 * 3600);
                                    _jedis.set(sceneStartLoadingKey, sceneStartLoading.toString());
                                    _jedis.expire(sceneStartLoadingKey, 24 * 3600);
                                    ifSceneStartLoadingSql = true;
                                }
                            }
                        }
                    }
                }
            } else if (11 == logs.length) {
                String area = logs[6];
                String areaPlayerNum = logs[7];
                String uname = logs[8];
                String cname = logs[9];
                String level = logs[10];

                if (token_gen.equals(token) && log_type.equals(LOG_ENTER_SCENE_SIGN)) {
                    String enterSceneListKey = "enterScene:" + game_abbr + ":" + platform_id + ":" + server_id + ":" + todayStr + ":set";

                    if (!_jedis.sismember(enterSceneListKey, uname)) {
                        //判断是否为新用户
                        String regkey = "greginfo-" + platform_id + "-" + game_abbr + "-" + server_id + "-" + uname;
                        if (_jedis.exists(regkey)) {
                            List userinfo = _jedis.lrange(regkey, 0, 0);
                            String[] userinfoArr = userinfo.get(0).toString().split("-");
                            if (userinfoArr.length == 6) {
                                String userRegDateStr = date.timestamp2str(Long.parseLong(userinfoArr[5]), "yyyyMMdd");
                                if (userRegDateStr.equals(todayStr)) {
                                    enterScene++;
                                    _jedis.sadd(enterSceneListKey, uname);
                                    _jedis.expire(enterSceneListKey, 30 * 24 * 3600);
                                    _jedis.set(enterSceneKey, enterScene.toString());
                                    _jedis.expire(enterSceneKey, 24 * 3600);
                                    ifEnterSceneSql = true;
                                }
                            }
                        }
                    }
                }
            }

            if (ifRegResLoadedSql || ifSceneStartLoadingSql || ifEnterSceneSql) {
                String ins_sql = String.format("INSERT INTO `opdata_recharge_remain` (platform, server, date, enter_regpage, before_enter_scene, after_enter_scene)" +
                                "VALUES (%s, %s, %d, %d, %d, %d) ON DUPLICATE KEY UPDATE enter_regpage=%d, before_enter_scene=%d, after_enter_scene=%d;",
                        platform_id, server_id, todayDate, regResLoaded, sceneStartLoading, enterScene, regResLoaded, sceneStartLoading, enterScene);

                //flush to mysql
                String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
                String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
                String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
                String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
                String mysql_passwd = _prop.getProperty("game." + game_abbr + ".mysql_passwd");
                System.out.println("@@@@@@ " + mysql_host + " " + mysql_port + " " + mysql_db + " " + mysql_user + " " + mysql_passwd);
                if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
                    boolean sql_ret;
                    try {
                        System.out.println(ins_sql);
                        sql_ret = _dbconnect.DirectUpdate(game_abbr, ins_sql);
                        System.out.println(sql_ret);
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("null"));
    }
}