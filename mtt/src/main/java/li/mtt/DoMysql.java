package li.mtt;
import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.PreparedStatement;  
import java.sql.SQLException;  
import java.sql.ResultSet;  



public class DoMysql {  
	  	
    static String sql = null;  
    static DBHelper db1 = null;  
    static ResultSet ret = null;  
    
    public static class DBHelper {  
        public static final String url = "jdbc:mysql://hadoop0/hive";  
        public static final String name = "com.mysql.jdbc.Driver";  
        public static final String user = "hive";  
        public static final String password = "mysql";  
      
        public Connection conn = null;  
        public PreparedStatement pst = null;  
      
        public DBHelper(String sql) {  
            try {  
                Class.forName(name);//指定连接类型  
                conn = DriverManager.getConnection(url, user, password);//获取连接  
                pst = conn.prepareStatement(sql);//准备执行语句  
            } catch (Exception e) {  
                e.printStackTrace();  
            }  
        }  
      
        public void close() {  
            try {  
                this.conn.close();  
                this.pst.close();  
            } catch (SQLException e) {  
                e.printStackTrace();  
            }  
        }  
    }
  
    public static void main(String[] args) {  
        sql = "select *from TBLS";//SQL语句  
        db1 = new DBHelper(sql);//创建DBHelper对象  
  
        try {  
            ret = db1.pst.executeQuery();//执行语句，得到结果集  
            while (ret.next()) {  
                String uid = ret.getString(1);  
                String ufname = ret.getString(2);  
                String ulname = ret.getString(3);  
                String udate = ret.getString(4);  
                System.out.println(uid + "\t" + ufname + "\t" + ulname + "\t" + udate );  
            }//显示数据  
            ret.close();  
            db1.close();//关闭连接  
        } catch (SQLException e) {  
            e.printStackTrace();  
        }  
    }  
    
    
  
}  
