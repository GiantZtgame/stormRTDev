package storm.qule_util;

import java.io.*;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/8/11.
 */
public class timerCfgLoader extends asyncTimer implements Serializable {
    //private asyncTimer _asyncTimer = new asyncTimer();

    public timerCfgLoader() {

    }

    public Properties loadCfg(String cfgPath, Properties formerProperties) {
        if (null == cfgPath || "".equals(cfgPath)) {
            System.out.println("@.@ - config file path empty!");
            System.exit(-1);
        }

        if (ifReachTimeout()) {
            Properties props = new Properties();
            InputStream game_cfg_in = null;
            try {
                game_cfg_in = new FileInputStream(cfgPath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(-10);
            }
            try {
                props.load(game_cfg_in);
            } catch (IOException e) {
                e.printStackTrace();
            }
            return props;
        } else {
            return formerProperties;
        }
    }
}
