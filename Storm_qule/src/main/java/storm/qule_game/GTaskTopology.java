package storm.qule_game;
/**
 * Created by zhanghang on 2014/7/15.
 */
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import storm.kafka.*;
import storm.qule_game.bolt.GTaskCalcBolt;
import storm.qule_game.bolt.GTaskVeriBolt;
import storm.qule_game.spout.GTaskSpout;

import java.io.IOException;


public class GTaskTopology {
    public static void main(String[] args) throws /*Exception*/AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("gtask_veri_bolt", new GTaskVeriBolt(),10).shuffleGrouping("gtask_spout");
        builder.setBolt("gtask_calc_bolt", new GTaskCalcBolt(),10).shuffleGrouping("gtask_veri_bolt");

        Config conf = new Config();
        conf.setDebug(true);
        //集群模式
        if (args != null && args.length > 0) {
            conf.put("isOnline",true);

            String gamecfg_path = "";
            try {
                gamecfg_path = args[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("NOTICE： 请输入游戏配置文件路径(param 2)!");
                e.printStackTrace();
                System.exit(-999);
            }
            conf.put("gamecfg_path", gamecfg_path);

            String topic = "gtask";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutId = "gtask";

            BrokerHosts zk = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConf = new SpoutConfig(zk, topic, zkRoot, spoutId);
            spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("gtask_spout", new KafkaSpout(spoutConf), 1);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        //本地模式
        } else {
            conf.put("isOnline",false);
            builder.setSpout("gtask_spout", new GTaskSpout());
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("gtask", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
