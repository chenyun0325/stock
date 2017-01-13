package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by cy111966 on 2016/11/24.
 */
public class DbConnection {

  public static Connection getConn(){
    Connection con=null;
    String driver="com.mysql.jdbc.Driver";
    String url="jdbc:mysql://localhost:3306/db_test?useUnicode=true&characterEncoding=utf-8";
    String username="root";
    String password="123456";
    try {
      Class.forName(driver);
      con = DriverManager.getConnection(url,username,password);
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return con;
  }


}
