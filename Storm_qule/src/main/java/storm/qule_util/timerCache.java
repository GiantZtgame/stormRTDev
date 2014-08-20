package storm.qule_util;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by WXF on 2014/7/16.
 */
public class timerCache {
    //缓存刷新时间(秒)
    private int _cache_timeout = 300;
    //private int _last_rec_time = Math.round(System.currentTimeMillis()/1000);
    private Map<String, Integer> _last_rec_time = new HashMap<String, Integer>();

    //private static boolean _flushing = false;

    private static BufferedReader _br = null;
    private static File _file = null;
    private static BufferedWriter _bw = null;
    private static String _filename_pre = "gonline_";
    private static String _file_path = "";

    private static mysql _dbconnect = null;

    Properties _prop = new Properties();

    public timerCache(int cache_timeout) {
        _cache_timeout = cache_timeout;
        _dbconnect = new mysql();

        InputStream game_cfg_in = getClass().getResourceAsStream("/config/games.properties");
        try {
            _prop.load(game_cfg_in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        _file_path = _prop.getProperty("game.intermediate_file.path");
    }

    //check if reach time point
    private boolean ifReachTimeout(String game_abbr) {
        if (null == _last_rec_time.get(game_abbr)) {
            _last_rec_time.put(game_abbr, Math.round(System.currentTimeMillis()/1000));
        }

        int cur_timestamp;

        cur_timestamp = Math.round(System.currentTimeMillis()/1000);

        if ( (cur_timestamp - _last_rec_time.get(game_abbr)) >= _cache_timeout ) {
            _last_rec_time.put(game_abbr, cur_timestamp);
            return true;
        } else {
            return false;
        }
    }

    public synchronized boolean push(String game_abbr, String sql) {
        //是否到达缓存刷新时间
        if (ifReachTimeout(game_abbr)/* && false == _flushing*/) {
            return false;
        } else {
            _file = new File(_file_path + _filename_pre + game_abbr);
            if (!_file.exists() != false) {
                try {
                    _file.createNewFile();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            try {
                _bw = new BufferedWriter(new FileWriter(_file, true));
                _bw.write(sql);
                _bw.newLine();
                _bw.flush();
                return true;
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    _bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return false;
    }

    public synchronized boolean pullAndFlush(String game_abbr) {
System.out.println("@@@###now into pullAndFlush aspect");
        //_flushing = true;
        StringBuilder result = new StringBuilder();

        try {
            FileReader fr = new FileReader(_file_path + _filename_pre + game_abbr);
            _br = new BufferedReader(fr);
            String b;

            while ((b = _br.readLine()) != null) {
                result.append(b);
            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                _br.close();
                //清空缓存文件
                BufferedWriter dbw;
                try {
                    _file = new File(_file_path + _filename_pre + game_abbr);
                    if (!_file.exists() != false) {
                        System.out.println("file: " + _file_path + _filename_pre + game_abbr + " not existed! Now stop flushing data into mysql.");
                        return false;
                    }
                    dbw = new BufferedWriter(new FileWriter(_file));
                    dbw.write("");
                    dbw.flush();
                } catch (IOException e) {
                    e.printStackTrace();
                } finally {
                    /*try {
                        dbw.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }*/
                }

                //_flushing = false;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

System.out.println(result.toString());

        //flush to mysql
        String mysql_host = _prop.getProperty("game." + game_abbr + ".mysql_host");
        String mysql_port = _prop.getProperty("game." + game_abbr + ".mysql_port");
        String mysql_db = _prop.getProperty("game." + game_abbr + ".mysql_db");
        String mysql_user = _prop.getProperty("game." + game_abbr + ".mysql_user");
        String mysql_passwd= _prop.getProperty("game." + game_abbr + ".mysql_passwd");
System.out.println(mysql_host + " " + mysql_port + " " + mysql_db + " " + mysql_user + " " + mysql_passwd);
        if (mysql.getConnection(game_abbr, mysql_host, mysql_port, mysql_db, mysql_user, mysql_passwd)) {
            boolean sql_ret = false;
            try {
System.out.println(result.toString());
                sql_ret = _dbconnect.DirectUpdate(game_abbr, result.toString());
            } catch (SQLException e) {
                e.printStackTrace();
            }
            return sql_ret;
        }

        return false;
    }

}
