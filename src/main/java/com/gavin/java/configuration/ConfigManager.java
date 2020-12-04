package java.com.gavin.java.configuration;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.com.gavin.java.exception.EtlException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * author:gavin
 * info:加载配置文件
 * time:2020-11-19
 */
public class ConfigManager {
    final private static Logger LOG = LoggerFactory.getLogger(ConfigManager.class);
    private static Properties prop = null;
    /*
    * 装载配置文件
    * */
    static {
        try{
            LOG.info("Jdbc Home:"+ FilenameUtils.concat(System.getProperty("conf"),"databus.properties"));
            File jdbcFile = new File(FilenameUtils.concat(System.getProperty("conf"),"databus.properties"));
            if(!jdbcFile.exists()){
                throw new EtlException("conf目录下无databus.properties配置文件!!!");
            }

            InputStream jdbcInput = new BufferedInputStream(new FileInputStream(jdbcFile));
            prop.load(jdbcInput);
            jdbcInput.close();
            LOG.info("DataBus properties load success..");
        }catch(Exception e){
            e.printStackTrace();
            throw new EtlException(e);
        }
    }

    /**
     * 获得相关参数值
     * @param key
     * @return
     */
    public static String getProperty(String key){return prop.getProperty(key);}

    public static String getProperty(String key,String defaultVal){
        String val = prop.getProperty(key);
        if(StringUtils.isBlank(val)){
            return val;
        }else{
            return defaultVal;
        }
    }

    /**
     * 跑批配置库
     * @return
     */
    public static Properties getTabConfInfo(){
        Properties prop = new Properties();
        String dbConfUser = ConfigManager.getProperty("confDB_user");
        String dbConfPass = ConfigManager.getProperty("confDB_password");
        prop.setProperty("user",dbConfPass);
        prop.setProperty("password",dbConfPass);
        return prop;
    }

    public static Properties getConfDBInfo(String dbName){
        Properties prop = new Properties();
        String dbUser = ConfigManager.getProperty(dbName+"_user");
        String dbPassword = ConfigManager.getProperty(dbName+"_password");
        if(dbUser != null && dbPassword != null){
            prop.setProperty("user",dbUser);
            prop.setProperty("password",dbPassword);
        }
        return prop;
    }
}
