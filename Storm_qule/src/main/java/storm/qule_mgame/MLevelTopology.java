package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MLevelCalcBolt;
import storm.qule_mgame.bolt.MLevelVeriBolt;
import storm.qule_mgame.spout.SampleMLevelupSpout;

/**
 * Created by wangxufeng on 2014/12/9.
 */
public class MLevelTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //等级
        builder.setBolt("mlevel_verify_bolt", new MLevelVeriBolt(), 10).shuffleGrouping("mlevel_spout");
        builder.setBolt("mlevel_calc_bolt", new MLevelCalcBolt(), 10).shuffleGrouping("mlevel_verify_bolt");

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
            String topicLevel = "mlevel";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdLevel = "mlevel";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfLevel = new SpoutConfig(brokerHosts, topicLevel, zkRoot, spoutIdLevel);
            spoutConfLevel.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mlevel_spout", new KafkaSpout(spoutConfLevel), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mlevel_spout", new SampleMLevelupSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
