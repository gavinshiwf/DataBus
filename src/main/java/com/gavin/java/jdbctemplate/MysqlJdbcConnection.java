package com.gavin.java.jdbctemplate;

import com.gavin.java.configuration.ConfigManager;
import com.gavin.java.exception.EtlException;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * author:gavin
 * time:2020-12-04
 */
public class MysqlJdbcConnection {
    private static final Logger LOG = LoggerFactory.getLogger(MysqlJdbcConnection.class);

    public JdbcConnectionFactory getConnectionFactory(String url, String userName, String password){
        return new JdbcConnectionFactory(DataBaseType.Mysql,url,userName,password);
    }

    public int updateLog(String sql,String dbName,Object...args){
        Connection conn = null;
        try{
            QueryRunner runner = new QueryRunner();
            String url = ConfigManager.getProperty(dbName);
            String userName = ConfigManager.getProperty(dbName+"_user");
            String passWord = ConfigManager.getProperty(dbName+"_password");

            conn = getConnectionFactory(url,userName,passWord).getConnection();
            conn.setAutoCommit(true);
            return runner.update(conn,sql,args);
        }catch(Exception e){
            throw new EtlException(e);
        }finally {
            closeDBConn(conn);
        }
    }

    /**
     * 返回一个查询结果
     * @param sql
     * @param dbName
     * @return
     * @throws SQLException
     */
    public List<String> sqlQuery(String sql,String dbName) throws SQLException{
        String url = ConfigManager.getProperty(dbName);
        String userName = ConfigManager.getProperty(dbName+"_user");
        String passWord = ConfigManager.getProperty(dbName+"_password");

        Connection conn = getConnectionFactory(url,userName,passWord).getConnection();
        List<String> list = new ArrayList<>();
        if(conn == null){
            throw new SQLException("Null Connection Exception");
        }else if(sql == null){
            throw new SQLException("Null SQL Exception");
        }else{
            try{
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                while(rs.next()){
                    list.add(rs.getString("table_name")+","+rs.getLong("modify_count")+","+rs.getLong("row_count"));
                }
            }catch(Exception e){
                throw new EtlException(e);
            }finally {
                closeDBConn(conn);
            }
        }
        return list;
    }

    /**
     * 执行DML
     * @param sql
     * @param dbName
     * @throws SQLException
     */
    public void executeSQL(String sql,String dbName) throws SQLException{
        String url = ConfigManager.getProperty(dbName);
        String userName = ConfigManager.getProperty(dbName+"_user");
        String passWord = ConfigManager.getProperty(dbName+"_password");
        Connection conn = getConnectionFactory(url,userName,passWord).getConnection();
        if(conn == null){
            throw new SQLException("Null Connection Exception");
        }else if(sql == null){
            throw new SQLException("Null SQL Exception");
        }else{
            try{
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.executeUpdate();
            }catch(Exception e){
                throw new EtlException(e);
            }finally {
                closeDBConn(conn);
            }
        }
    }

    public String queryVal(String sql,String dbName) throws SQLException{
        String url = ConfigManager.getProperty(dbName);
        String userName = ConfigManager.getProperty(dbName+"_user");
        String passWord = ConfigManager.getProperty(dbName+"_password");
        Connection conn = getConnectionFactory(url,userName,passWord).getConnection();

        String returnVal = null;
        if(conn == null){
            throw new SQLException("Null Connection Exception");
        }else if(sql == null){
            throw new SQLException("Null SQL Exception");
        }else{
            try{
                PreparedStatement stmt = conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery();
                while(rs.next()){
                    returnVal = rs.getString(1);
                }
            }catch(Exception e){
                throw new EtlException(e);
            }finally {
                closeDBConn(conn);
            }
        }
        return returnVal;
    }

    /***
     * 关闭连接
     * @param conn
     */
    public void closeDBConn(Connection conn){
        if(null != conn){
            try{
                conn.close();
            }
            catch(SQLException e){
                LOG.error("close jdbc connection error"+e);
            }
        }
    }

    /**
     * 获取一个连接
     * @param dbName
     * @return
     */
    public Connection getConnection(String dbName){
        String url = ConfigManager.getProperty(dbName);
        String userName = ConfigManager.getProperty(dbName+"_user");
        String passWord = ConfigManager.getProperty(dbName+"_password");
        Connection conn = getConnectionFactory(url,userName,passWord).getConnection();
        return conn;
    }

    /**
     * 开始一个事务
     * @param conn
     */
    public void beginTransaction(Connection conn){
        if(conn != null){
            try{
                conn.setAutoCommit(false);
            }catch(SQLException e){
                LOG.error("begin transaction error .." + e);
            }
        }
    }

    /**
     * 获取一个statement
     * @param conn
     * @return
     * @throws SQLException
     */
    public Statement getStat(Connection conn) throws SQLException{
        Statement stmt = conn.createStatement();
        return stmt;
    }
}
