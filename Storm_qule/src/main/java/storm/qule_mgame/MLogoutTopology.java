package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MLogoutCalcBolt;
import storm.qule_mgame.bolt.MLogoutVeriBolt;
import storm.qule_mgame.spout.SampleMLogoutSpout;

/**
 * Created by wangxufeng on 2014/11/30.
 */
public class MLogoutTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //登出
        builder.setBolt("mlogout_verify_bolt", new MLogoutVeriBolt(), 10).shuffleGrouping("mlogout_spout");
        builder.setBolt("mlogout_calc_bolt", new MLogoutCalcBolt(), 10).shuffleGrouping("mlogout_verify_bolt");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            String gamecfg_path = "";
            try {
                gamecfg_path = args[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("NOTICE： 请输入游戏配置文件路径(param 2)!");
                e.printStackTrace();
                System.exit(-999);
            }
            conf.put("gamecfg_path", gamecfg_path);

            conf.put("isOnline",true);
            String topicLogout = "mlogout";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdLogout = "mlogout";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfLogout = new SpoutConfig(brokerHosts, topicLogout, zkRoot, spoutIdLogout);
            spoutConfLogout.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mlogout_spout", new KafkaSpout(spoutConfLogout), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mlogout_spout", new SampleMLogoutSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
