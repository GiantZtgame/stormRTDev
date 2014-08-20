package storm.qule_game;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;
import storm.qule_game.bolt.*;
import storm.qule_game.spout.SampleGloginSpout;
import storm.qule_game.spout.SampleGloginreqSpout;
import storm.qule_game.spout.SampleGregSpout;

/**
 * Created by wangxufeng on 2014/7/18.
 */
public class GpLoginTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //请求登录
        builder.setBolt("gloginreq_verify_bolt", new GLoginreqVeriBolt(), 1).shuffleGrouping("gloginreq_spout");
        builder.setBolt("gloginreq_calc_bolt", new GLoginreqCalcBolt(), 1).shuffleGrouping("gloginreq_verify_bolt");

        //登录
        builder.setBolt("glogin_verify_bolt", new GLoginVeriBolt(), 1).shuffleGrouping("glogin_spout");
        builder.setBolt("glogin_calc_bolt", new GLoginCalcBolt(), 1).shuffleGrouping("glogin_verify_bolt");
        //广告登录
        builder.setBolt("glogin_adthru_calc_bolt", new GAdthruLoginCalcBolt(), 1).fieldsGrouping("glogin_verify_bolt", new Fields("game_abbr", "platform_id", "server_id"));
        builder.setBolt("glogin_adthru_crud_bolt", new GAdthruLoginCRUDBolt(), 1).fieldsGrouping("glogin_adthru_calc_bolt", new Fields("game_abbr"));

        //建角
        builder.setBolt("greg_verify_bolt", new GRegVeriBolt(), 1).shuffleGrouping("greg_spout");
        builder.setBolt("greg_calc_bolt", new GRegCalcBolt(), 1).fieldsGrouping("greg_verify_bolt", new Fields("game_abbr", "platform_id", "server_id"));
        builder.setBolt("greg_crud_bolt", new GRegCRUDBolt(), 1).fieldsGrouping("greg_calc_bolt", new Fields("game_abbr"));
        //广告建角
        builder.setBolt("greg_adthru_calc_bolt", new GAdthruGRegCalcBolt(), 1).fieldsGrouping("greg_verify_bolt", new Fields("game_abbr", "platform_id", "server_id"));
        builder.setBolt("greg_adthru_crud_bolt", new GAdthruGRegCRUDBolt(), 1).fieldsGrouping("greg_adthru_calc_bolt", new Fields("game_abbr"));

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
            String topicLoginReq = "gloginreq";
            String topicLogin = "glogin";
            String topicReg = "greg";
            String zkRoot = "/home/ztgame/storm/zkroot";
            String spoutIdLoginReq = "gloginreq";
            String spoutIdLogin = "glogin";
            String spoutIdReg = "greg";
            BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");
            SpoutConfig spoutConfLoginReq = new SpoutConfig(brokerHosts, topicLoginReq, zkRoot, spoutIdLoginReq);
            spoutConfLoginReq.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("gloginreq_spout", new KafkaSpout(spoutConfLoginReq), 1);

            SpoutConfig spoutConfLogin = new SpoutConfig(brokerHosts, topicLogin, zkRoot, spoutIdLogin);
            spoutConfLogin.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("glogin_spout", new KafkaSpout(spoutConfLogin), 1);

            SpoutConfig spoutConfReg = new SpoutConfig(brokerHosts, topicReg, zkRoot, spoutIdReg);
            spoutConfReg.scheme = new SchemeAsMultiScheme(new StringScheme());
            builder.setSpout("greg_spout", new KafkaSpout(spoutConfReg), 1);

            conf.setNumWorkers(1);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.put("isOnline",false);
            builder.setSpout("gloginreq_spout", new SampleGloginreqSpout(), 1);
            builder.setSpout("glogin_spout", new SampleGloginSpout(), 1);
            builder.setSpout("greg_spout", new SampleGregSpout(), 1);

            conf.setMaxTaskParallelism(2);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());
            Thread.sleep(200000);
            cluster.shutdown();
        }
    }
}
