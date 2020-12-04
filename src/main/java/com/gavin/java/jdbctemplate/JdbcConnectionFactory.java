package java.com.gavin.java.jdbctemplate;

import java.com.gavin.java.exception.EtlException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.Callable;

/**
 * author:gavin
 * time:2020-12-03
 * jdbc 连接工厂
 */
public class JdbcConnectionFactory {
    private final String jdbcUrl;
    private final String userName;
    private final String password;
    private final DataBaseType dataBaseType;

    public JdbcConnectionFactory(DataBaseType dataBaseType,
                                 String jdbcUrl,
                                 String userName,
                                 String password){
        this.dataBaseType = dataBaseType;
        this.jdbcUrl = jdbcUrl;
        this.userName = userName;
        this.password = password;
    }

    public Connection getConnection(){
        try{
            return RetryUtil.executeWithRetry(new Callable<Connection>() {
                public Connection call() throws Exception {
                    return connect(dataBaseType,jdbcUrl,userName,password);
                }
            },3,1000L,true);
        }catch(Exception e){
            throw new EtlException(e);
        }
    }

    private static Connection connect(DataBaseType dataBaseType,
                                      String url,
                                      String user,
                                      String pass){
        Properties prop = new Properties();
        prop.put("user",user);
        prop.put("password",pass);
        return connect(dataBaseType,url,prop);
    }

    private static Connection connect(DataBaseType dataBaseType,String url,Properties prop){
        try{
            Class.forName(dataBaseType.getDriverClassName());
            return DriverManager.getConnection(url,prop);
        }catch(Exception e){
            throw new EtlException(e);
        }
    }
}
