package storm.qule_game.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.AckFailDelegate;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.InprocMessaging;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Created by zhanghang on 2014/7/21.
 */
public class FeederSpout extends BaseRichSpout {
    private int _id;
    private Fields _outFields;
    private SpoutOutputCollector _collector;
    private AckFailDelegate _ackFailDelegate;

    public FeederSpout(Fields outFields) {
        //_id=1
        _id = InprocMessaging.acquireNewPort();
        _outFields = outFields;
    }

    public void setAckFailDelegate(AckFailDelegate d) {
        _ackFailDelegate = d;
    }

    public void feed(List<Object> tuple) {
        //tuple1 = [0,"male"]
        //tuple2 = [0,20]
        feed(tuple, UUID.randomUUID().toString());
    }

    public void feed(List<Object> tuple, Object msgId) {
        //msgId = "09768e22-c5ee-438e-8d02-76aa6bf05a1b";
        InprocMessaging.sendMessage(_id, new Values(tuple, msgId));
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
        List<Object> toEmit = (List<Object>) InprocMessaging.pollMessage(_id);
        if(toEmit!=null) {
            List<Object> tuple = (List<Object>) toEmit.get(0);
            Object msgId = toEmit.get(1);

            _collector.emit(tuple, msgId);
        } else {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void ack(Object msgId) {
        if(_ackFailDelegate!=null) {
            _ackFailDelegate.ack(msgId);
        }
    }

    public void fail(Object msgId) {
        if(_ackFailDelegate!=null) {
            _ackFailDelegate.fail(msgId);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_outFields);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new HashMap<String, Object>();
    }
}
