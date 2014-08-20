package storm.qule_util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by wangxufeng on 2014/8/11.
 */
public class cfgLoader {
    public cfgLoader() {

    }

    public Properties loadConfig(String cfgPath) {
        boolean ifOuterRes = false;
        return loadConfig(cfgPath, ifOuterRes);
    }

    public Properties loadConfig(String cfgPath, boolean ifOuterRes) {
        Properties prop = new Properties();
        InputStream game_cfg_in = null;

        if (false == ifOuterRes) {
            game_cfg_in = getClass().getResourceAsStream(cfgPath);
        } else {
            try {
                game_cfg_in = new FileInputStream(cfgPath);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                System.exit(-10);
            }
        }

        try {
            prop.load(game_cfg_in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return prop;
    }
}
