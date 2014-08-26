package storm.qule_util;

import java.sql.*;

import clojure.lang.Obj;
import com.mysql.jdbc.Connection;
import com.mysql.jdbc.exceptions.jdbc4.CommunicationsException;

import java.util.*;

public class JdbcMysql {
    private Connection conn =null;
    private static String HOST;
    private static String PORT;
    private static String DB;
    private static String USER;
    private static String PASSWD;
    private Statement statement =null;
    private ResultSet result=null;
    private static JdbcMysql __instance = null;
    static Map<String,JdbcMysql> Instance = new HashMap<String,JdbcMysql>();
    private boolean b;
    private int a;

    public static JdbcMysql getInstance(String game_abbr, String host,String port,String db, String user, String passwd) {
        HOST = host;
        PORT = port;
        DB = db;
        USER = user;
        PASSWD = passwd;
        __instance = Instance.get(game_abbr);
        if(__instance == null) {
            synchronized (JdbcMysql.class) {
                System.out.println("new instance");
                __instance = new JdbcMysql();
                Instance.put(game_abbr, __instance);
            }
        }
        return __instance;
    }

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private JdbcMysql(){
        try {
            //加载驱动
            Class.forName("com.mysql.jdbc.Driver");
            conn = (Connection) DriverManager.getConnection("jdbc:mysql://"+HOST+":"+PORT+"/"+DB+"?useUnicode=true&characterEncoding=utf8",USER,PASSWD);
            System.out.println("加载驱动成功！！");
        }catch (ClassNotFoundException e) {
            System.out.println("加载驱动失败！！");
            e.printStackTrace();
        }catch (SQLException e) {
            System.out.println("获取数据库连接失败！！");
            e.printStackTrace();
        }
    }
    //增加数据  
    public boolean add(String sql){
        try {
            statement =conn.createStatement();
            a=statement.executeUpdate(sql);
        }catch (SQLException e) {
            e.printStackTrace();
        }
        return a >0 ?true:false;
    }

    //批量处理
    public boolean batchAdd(List sqls) {
        try {
            conn.setAutoCommit(false);
            statement =conn.createStatement();
            for (int i =0;i<sqls.size();i++) {
                a = statement.executeUpdate(sqls.get(i).toString());
            }
            conn.commit();
        }catch (SQLException e){
            e.printStackTrace();
        }
        return a > 0?true:false;
    }
    //删除数据  
    public int delete(String sql){
        try {
            statement =conn.createStatement();
            a=statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return a;
    }

    //查询数据  
    public ResultSet select(String sql){
        try {
            Statement statement =conn.createStatement(
                    ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_UPDATABLE);
            result=statement.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return result;
    }

    //改数据  
    public int update(String sql){
        try {
            statement =conn.createStatement();
            a=statement.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return a;
    }

    //关闭资源  
    public boolean close(){
        try {
            result.close();
            statement.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return b;
    }

    //生成sql语句
   public String setSql(String type, String tbname,Map<String,Map<String,Object>>sqls) {
        String sql = "";
        if (type.equals("replace")) {
            Map<String,Object> insert = sqls.get("insert");
            Map<String,Object> update = sqls.get("update");
            sql = "INSERT INTO `"+tbname+"` SET "+sqlSet(insert)+
                    " ON DUPLICATE KEY UPDATE "+sqlSet(update);
        }
        if (type.equals("insert")) {
            Map<String,Object> insert = sqls.get("insert");
            sql = "INSERT INTO `"+tbname+"` SET "+sqlSet(insert);
        }
        return sql;
    }
    private static String sqlSet(Map<String,Object>sql) {
        String set= "";
        Iterator iter = sql.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            String val = entry.getValue().toString();
            if (!isNum(val)){
                val = "\""+val+"\"";
            }
            set += ",`" + key + "` = "+val;
        }
        return set.substring(1,set.length());
    }
    //2、java自带函数
    public static boolean isNum(String str){
        for (int i = str.length();--i>=0;){
            if (!Character.isDigit(str.charAt(i))){
                return false;
            }
        }
        return true;
    }
}  