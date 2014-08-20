package storm.qule_util;

import com.mysql.jdbc.CommunicationsException;

import javax.swing.plaf.nimbus.State;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by WXF on 2014/7/16.
 */
public class mysql {
    private final String DRIVER="com.mysql.jdbc.Driver";

    static Map<String, Connection> _conns = new HashMap<String, Connection>();
    static Map<String, String> _existConns = new HashMap<String, String>();

    static boolean  _conn = false;
    static Statement _st;

    // 定义sql语句的执行对象
    private static PreparedStatement _pstmt;
    // 定义查询返回的结果集合
    private static ResultSet _rst;

    public mysql() {
        if (!mysql._conn) {
            try {
System.out.println("===========================mysql注册");
                Class.forName(DRIVER);
            } catch (Exception e) {
System.out.println("数据库加载失败！");
            }
        }
    }

    public synchronized static boolean getConnection(String game_abbr, String mysql_host, String port, String db, String user, String passwd) {
        //if (null == mysql._existConns.get(game_abbr+mysql_host+port+db+user+passwd)) {
        if (null == mysql._existConns.get(game_abbr+"_mysqlConfig") || !mysql._existConns.get(game_abbr+"_mysqlConfig").equals(game_abbr+"##"+mysql_host+"##"+port+"##"+db+"##"+user+"##"+passwd)) {
            try {
System.out.println("==========================mysql创建连接实例");
                mysql._conns.put(game_abbr, DriverManager.getConnection(
                        "jdbc:mysql://" + mysql_host + ":" + port + "/" + db, user, passwd) );
                mysql._conns.get(game_abbr).setAutoCommit(true);
                mysql._conn = true;
                mysql._existConns.put(game_abbr+"_mysqlConfig", game_abbr+"##"+mysql_host+"##"+port+"##"+db+"##"+user+"##"+passwd);
                //mysql._existConns.put(game_abbr+mysql_host+port+db+user+passwd, true);
            } catch (Exception e) {
System.out.println("数据库连接失败:" + e.getMessage());
                return false;
            }
        }
        return true;
    }

    //update/insert/delete
    public static boolean executeUpdate(String game_abbr, String sql, String params[]) throws SQLException {
        boolean flag;
        int result;

        _pstmt =mysql._conns.get(game_abbr).prepareStatement(sql);

        //fulfill placeholder
        if(params!=null && params.length != 0) {
            for(int i=0; i<params.length; i++){
                _pstmt.setString(i+1, params[i]);
            }
        }

        result = _pstmt.executeUpdate();
        flag = result > 0 ? true : false;

        //_conn.close();
        return flag;
    }

    //select
    public static ResultSet executeQuery(String game_abbr, String sql, String params[]) throws SQLException{
        _pstmt=mysql._conns.get(game_abbr).prepareStatement(sql);

        //fulfill placeholder
        if(params!=null && params.length != 0) {
            for(int i=0; i<params.length; i++){
                _pstmt.setString(i+1, params[i]);
            }
        }
        _rst=_pstmt.executeQuery();

        //_conn.close();
        return _rst;
    }

    //direct insert/update/delete multiple sqls without prepare statement
    public static boolean DirectUpdateBatch(String game_abbr, String sql) throws SQLException {
        String sqls [];
        sqls = sql.split(";");

        _st = mysql._conns.get(game_abbr).createStatement();

        int i;
        for(i=0; i<sqls.length; i++) {
            _st.addBatch(sqls[i]);
        }

        //int count = _st.executeUpdate(sql);
        int [] count = _st.executeBatch();
System.out.println("------------------------------executeBatch result: " + count.length);
        mysql._conns.get(game_abbr).close();

        return count.length>0;
    }

    //direct insert/update/delete multiple sqls without prepare statement
    public static boolean DirectUpdate(String game_abbr, String sql) throws SQLException {
        int count = 0;
        int tryCounts = 0;
        do {
            try {
                _st = mysql._conns.get(game_abbr).createStatement();
                count = _st.executeUpdate(sql);
            } catch (CommunicationsException e) {
                e.printStackTrace();
                System.out.println("====================communicationException detected! try to reconnect!");
                String dbCfg = mysql._existConns.get(game_abbr+"_mysqlConfig");
                String[] dbCfgArr = dbCfg.split("##");
                mysql._conns.put(game_abbr, DriverManager.getConnection(
                        "jdbc:mysql://" + dbCfgArr[1] + ":" + dbCfgArr[2] + "/" + dbCfgArr[3], dbCfgArr[4], dbCfgArr[5]) );
                mysql._conns.get(game_abbr).setAutoCommit(true);
                tryCounts++;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } while (tryCounts > 0 && tryCounts < 3);
System.out.println("------------------------------execute result: " + count);
       // _conn.close();

        return count>0;
    }
}
