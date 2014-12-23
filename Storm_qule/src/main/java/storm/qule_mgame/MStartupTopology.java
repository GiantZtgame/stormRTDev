package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MStartupCalcBolt;
import storm.qule_mgame.bolt.MStartupVeriBolt;
import storm.qule_mgame.spout.SampleMStartupSpout;

/**
 * Created by wangxufeng on 2014/11/24.
 */
public class MStartupTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //启动
        builder.setBolt("mstartup_verify_bolt", new MStartupVeriBolt(), 10).shuffleGrouping("mstartup_spout");
        builder.setBolt("mstartup_calc_bolt", new MStartupCalcBolt(), 10).shuffleGrouping("mstartup_verify_bolt");

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
            String topicStartup = "mstartup";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdStartup = "mstartup";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfStartup = new SpoutConfig(brokerHosts, topicStartup, zkRoot, spoutIdStartup);
            spoutConfStartup.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mstartup_spout", new KafkaSpout(spoutConfStartup), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mstartup_spout", new SampleMStartupSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
