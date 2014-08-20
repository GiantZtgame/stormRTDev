package storm.qule_game.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by wangxufeng on 2014/7/21.
 */
public class GunionLoginRegBolt extends BaseBasicBolt {
    public GunionLoginRegBolt() {
    }

    @Override
    public void prepare(Map conf, TopologyContext context) {

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple.getString(0));
        System.out.println(tuple.getString(1));
        System.out.println(tuple.getValues());
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("test"));
    }
}
