package storm.qule_util;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Created by wangxufeng on 2014/7/15.
 */
public class md5 {
    private static final String ALGORITHM = "MD5";
    MessageDigest _mcrypt = null;

    public  md5() throws NoSuchAlgorithmException {
        _mcrypt = MessageDigest.getInstance(ALGORITHM);

    }

    public String gen_md5(String raw_str) {
        if (null == raw_str) {
            return "";
        }

        _mcrypt.reset();
        _mcrypt.update(raw_str.getBytes());
        byte[] digest = _mcrypt.digest();
        BigInteger bigInt = new BigInteger(1, digest);
        String hashtext = bigInt.toString(16);
        // Now we need to zero pad it if you actually want the full 32 chars.
        while(hashtext.length() < 32 ){
            hashtext = "0"+hashtext;
        }

        return hashtext;
    }
}
