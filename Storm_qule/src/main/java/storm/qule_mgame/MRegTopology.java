package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MRegCalcBolt;
import storm.qule_mgame.bolt.MRegVeriBolt;
import storm.qule_mgame.spout.SampleMRegSpout;

/**
 * Created by wangxufeng on 2015/1/30.
 */
public class MRegTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //等级
        builder.setBolt("mreg_verify_bolt", new MRegVeriBolt(), 10).shuffleGrouping("mreg_spout");
        builder.setBolt("mreg_calc_bolt", new MRegCalcBolt(), 10).shuffleGrouping("mreg_verify_bolt");

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
            String topicReg = "mreg";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdReg = "mreg";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfReg = new SpoutConfig(brokerHosts, topicReg, zkRoot, spoutIdReg);
            spoutConfReg.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mreg_spout", new KafkaSpout(spoutConfReg), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mreg_spout", new SampleMRegSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
