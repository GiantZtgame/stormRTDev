package storm.qule_game;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_game.bolt.*;
import storm.qule_game.spout.SampleGloginSpout;
/**
 * Created by zhanghang on 2014/7/29.
 */
public class GDailyRemainTopology {
    public static void main(String[]    args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt("dremain_verify_bolt", new GLoginVeriBolt(), 1).shuffleGrouping("dailyremain_spout");
        builder.setBolt("dremain_calc_bolt", new GDailyRemainBolt(), 1).shuffleGrouping("dremain_verify_bolt");

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
            String topic = "glogin";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdDailyRemain = "glogin";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfLogin = new SpoutConfig(brokerHosts, topic, zkRoot, spoutIdDailyRemain);
            spoutConfLogin.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("dailyremain_spout", new KafkaSpout(spoutConfLogin), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.put("isOnline",false);
            builder.setSpout("dailyremain_spout", new SampleGloginSpout(), 1);
            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
