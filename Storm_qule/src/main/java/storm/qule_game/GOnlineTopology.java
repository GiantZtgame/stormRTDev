package storm.qule_game;

import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import storm.kafka.*;
import storm.qule_game.bolt.GOnlineCRUDBolt;
import storm.qule_game.bolt.GOnlinePeakCRUDBolt;
import storm.qule_game.bolt.GOnlinePeakCalcBolt;
import storm.qule_game.bolt.GOnlineVeriBolt;
import storm.qule_game.spout.SampleGOnlineSpout;

/**
 * Created by wangxufeng on 2014/7/15.
 */
public class GOnlineTopology {
    public static void main(String[] args) throws Exception {
        String topic = "gonline";
        String zkRoot = "/home/ztgame/storm/zkroot";
        String spoutId = "gonline";
        BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setBolt("gonline_verify_bolt", new GOnlineVeriBolt(), 10).shuffleGrouping("gonline_spout");
        builder.setBolt("gonline_crud_bolt", new GOnlineCRUDBolt(), 10).fieldsGrouping("gonline_verify_bolt", new Fields("game_abbr"));

        builder.setBolt("gonlinePeak_calc_bolt", new GOnlinePeakCalcBolt(), 10).fieldsGrouping("gonline_verify_bolt", new Fields("game_abbr", "platform_id", "server_id"));
        builder.setBolt("gonlinePeak_crud_bolt", new GOnlinePeakCRUDBolt(), 10).fieldsGrouping("gonlinePeak_calc_bolt", new Fields("game_abbr"));

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
            builder.setSpout("gonline_spout", new KafkaSpout(spoutConf), 1);

            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.put("isOnline",false);
            builder.setSpout("gonline_spout", new SampleGOnlineSpout(), 1);

            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(10000);
            cluster.shutdown();
        }
    }
}
