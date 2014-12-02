package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MObjCalcBolt;
import storm.qule_mgame.bolt.MObjVeriBolt;
import storm.qule_mgame.spout.SampleMObjSpout;

/**
 * Created by wangxufeng on 2014/12/1.
 */
public class MObjTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //道具变动
        builder.setBolt("mobj_verify_bolt", new MObjVeriBolt(), 10).shuffleGrouping("mobj_spout");
        builder.setBolt("mobj_calc_bolt", new MObjCalcBolt(), 10).shuffleGrouping("mobj_verify_bolt");

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
            String topicObj = "mobj";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdObj = "mobj";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfObj = new SpoutConfig(brokerHosts, topicObj, zkRoot, spoutIdObj);
            spoutConfObj.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mobj_spout", new KafkaSpout(spoutConfObj), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mobj_spout", new SampleMObjSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
