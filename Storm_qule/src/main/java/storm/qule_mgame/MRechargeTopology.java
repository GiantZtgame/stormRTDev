package storm.qule_mgame;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_mgame.bolt.MBillCalcBolt;
import storm.qule_mgame.bolt.MBillVeriBolt;
import storm.qule_mgame.spout.SampleMBillSpout;

/**
 * Created by wangxufeng on 2014/11/25.
 */
public class MRechargeTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        //充值
        builder.setBolt("mbill_verify_bolt", new MBillVeriBolt(), 10).shuffleGrouping("mbill_spout");
        builder.setBolt("mbill_calc_bolt", new MBillCalcBolt(), 10).shuffleGrouping("mbill_verify_bolt");

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
            String topicOnline = "mbill";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdOnline = "mbill";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

            SpoutConfig spoutConfOnline = new SpoutConfig(brokerHosts, topicOnline, zkRoot, spoutIdOnline);
            spoutConfOnline.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("mbill_spout", new KafkaSpout(spoutConfOnline), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline", false);
            builder.setSpout("mbill_spout", new SampleMBillSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
