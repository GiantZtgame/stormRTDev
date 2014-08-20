package storm.qule_game;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_game.bolt.GSceneBolt;
import storm.qule_game.spout.SampleGSceneSpout;

/**
 * Created by wangxufeng on 2014/8/19.
 */
public class GSceneTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("gscene_bolt", new GSceneBolt(), 5).shuffleGrouping("gscene_spout");

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
            String topicScene = "gscene";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdScene = "gscene";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConfScene = new SpoutConfig(brokerHosts, topicScene, zkRoot, spoutIdScene);
            spoutConfScene.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("gscene_spout", new KafkaSpout(spoutConfScene), 1);

            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.put("isOnline",false);
            builder.setSpout("gscene_spout", new SampleGSceneSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
