package storm.qule_game;
/**
 * Created by zhanghang on 2014/7/15.
 */
import storm.qule_game.bolt.GRechargeCalcBolt;
import storm.qule_game.bolt.GRechargeVeriBolt;
import storm.qule_game.spout.GRechargeSpout;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import java.io.IOException;
public class GRechargeTopology {
    public static void main(String[] args) throws /*Exception*/AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setBolt("gbill_veri_bolt", new GRechargeVeriBolt()).shuffleGrouping("gbill_spout");
        builder.setBolt("gbill_calc_bolt", new GRechargeCalcBolt()).shuffleGrouping("gbill_veri_bolt");

        Config conf = new Config();
        conf.setDebug(true);
        //集群模式
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
            String topic = "gbill";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutId = "gbill";
            BrokerHosts zk = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConf = new SpoutConfig(zk, topic, zkRoot, spoutId);
            spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("gbill_spout", new KafkaSpout(spoutConf), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        //本地模式
        } else {
            conf.put("isOnline",false);
            builder.setSpout("gbill_spout", new GRechargeSpout());
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("gbill", conf, builder.createTopology());
            Thread.sleep(100000);
            cluster.shutdown();
        }
    }
}
