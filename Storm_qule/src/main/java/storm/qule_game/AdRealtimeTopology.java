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

import storm.qule_game.bolt.*;
import storm.qule_game.spout.AdRealtimeSpout;

import java.io.IOException;
public class AdRealtimeTopology {
    public static void main(String[] args) throws /*Exception*/AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {
        TopologyBuilder builder = new TopologyBuilder();
        //过滤源数据
        builder.setBolt("adrealtime_filter_bolt", new AdRealtimeFilterBolt(),10).shuffleGrouping("adrealtime_spout");

        //处理数据
        builder.setBolt("adrealtime_calc_bolt", new AdRealtimeCalcBolt(),10).shuffleGrouping("adrealtime_filter_bolt");
        builder.setBolt("adref_calc_bolt", new AdRefCalcBolt(),14).shuffleGrouping("adrealtime_filter_bolt");

        //记录数据库
        builder.setBolt("adrealtime_mysql_bolt", new AdRealtimeMysqlBolt(),1).shuffleGrouping("adrealtime_calc_bolt");
        builder.setBolt("adref_mysql_bolt", new AdRefMysqlBolt(),1).shuffleGrouping("adref_calc_bolt");

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
            conf.put("isOnline", true);
            conf.put("gamecfg_path", gamecfg_path);
            conf.put("redis.host","172.29.201.205");
            conf.put("redis.port",6379);

            String topic = "adrealtime";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutId = "adrealtime";

            BrokerHosts zk = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConf = new SpoutConfig(zk, topic, zkRoot, spoutId);
            spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("adrealtime_spout", new KafkaSpout(spoutConf), 1);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        //本地模式
        } else {
            conf.put("isOnline", false);
            conf.put("gamecfg_path", "/config/test.games.properties");
            conf.put("redis.host","localhost");
            conf.put("redis.port",6379);

            builder.setSpout("adrealtime_spout", new AdRealtimeSpout());
            conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
            conf.setMaxTaskParallelism(1);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("adrealtime", conf, builder.createTopology());
            Thread.sleep(30000);
            cluster.shutdown();
        }
    }
}
