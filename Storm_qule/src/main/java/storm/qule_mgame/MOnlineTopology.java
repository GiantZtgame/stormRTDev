package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MOnlineCalcBolt;
import storm.qule_mgame.bolt.MOnlineVeriBolt;
import storm.qule_mgame.spout.SampleMOnlineSpout;

/**
 * Created by wangxufeng on 2014/11/25.
 */
public class MOnlineTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //在线
        builder.setBolt("monline_verify_bolt", new MOnlineVeriBolt(), 10).shuffleGrouping("monline_spout");
        builder.setBolt("monline_calc_bolt", new MOnlineCalcBolt(), 10).shuffleGrouping("monline_verify_bolt");

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
            String topicOnline = "monline";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdOnline = "monline";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfOnline = new SpoutConfig(brokerHosts, topicOnline, zkRoot, spoutIdOnline);
            spoutConfOnline.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("monline_spout", new KafkaSpout(spoutConfOnline), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("monline_spout", new SampleMOnlineSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
