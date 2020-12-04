package java.com.gavin.java.jdbctemplate;

/**
 * author:gavin
 * time:2020-12-03
 */
public enum DataBaseType {
    Mysql("mysql","com.mysql.jdbc.Driver"),
    Hive("hive","org.apache.hive.jdbc.HiveDriver");

    private String typeName;
    private String driverClassName;

    DataBaseType(String typeName,String driverClassName){
        this.typeName = typeName;
        this.driverClassName = driverClassName;
    }

    public String getDriverClassName(){return this.driverClassName;}
    public String getTypeName(){return this.typeName;}
}
