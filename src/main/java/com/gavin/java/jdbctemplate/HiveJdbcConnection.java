package java.com.gavin.java.jdbctemplate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.com.gavin.java.configuration.ConfigManager;
import java.com.gavin.java.exception.EtlException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * author:gavin
 * time:2020-12-03
 */
public class HiveJdbcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(HiveJdbcConnection.class);

    public JdbcConnectionFactory getConnectionFactory(String url,String userName,String password){
        return new JdbcConnectionFactory(DataBaseType.Hive,url,userName,password);
    }

    public boolean sqlQuery(String sql,String hiveDB) throws SQLException{
        String url = ConfigManager.getProperty(hiveDB);
        String userName = ConfigManager.getProperty(hiveDB+"_user");
        String passWord = ConfigManager.getProperty(hiveDB+"_password");
        Connection conn = getConnectionFactory(url,userName,passWord).getConnection();
        Boolean returnVal = false;
        if(conn == null){
            throw new SQLException("Null connection");
        }
        else if(sql == null){
            closeDBConn(conn);
            throw new SQLException("Null SQL statement");
        }else{
            try{
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.executeUpdate();
                returnVal = true;
            }catch(Exception e){
                throw new EtlException(e);
            }finally {
                closeDBConn(conn);
            }
        }
        return returnVal;
    }

    public void closeDBConn(Connection conn){
        if(null != conn){
            try{
                conn.close();
            }catch(SQLException e){
                LOG.error("close jdbc connection error..");
            }
        }
    }
}
