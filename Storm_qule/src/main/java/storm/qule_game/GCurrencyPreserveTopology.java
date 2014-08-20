package storm.qule_game;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_game.bolt.GCurrencyPreserveCRUDcBolt;
import storm.qule_game.bolt.GCurrencyPreserveVeriBolt;
import storm.qule_game.spout.SampleGCurrencyPreserve;

/**
 * Created by wangxufeng on 2014/8/11.
 */
public class GCurrencyPreserveTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("gcurrencypreserve_verify_bolt", new GCurrencyPreserveVeriBolt(), 1).shuffleGrouping("gcurrencypreserve_spout");
        builder.setBolt("gcurrencypreserve_crud_bolt", new GCurrencyPreserveCRUDcBolt(), 1).shuffleGrouping("gcurrencypreserve_verify_bolt");

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
            String topicCurrencyPreserve = "gcurrencypreserve";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdCurrencyPreserve = "gcurrencypreserve";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConfCurrencyPreserve = new SpoutConfig(brokerHosts, topicCurrencyPreserve, zkRoot, spoutIdCurrencyPreserve);
            spoutConfCurrencyPreserve.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("gcurrencypreserve_spout", new KafkaSpout(spoutConfCurrencyPreserve), 1);

            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.put("isOnline",false);
            builder.setSpout("gcurrencypreserve_spout", new SampleGCurrencyPreserve(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
