package storm.qule_game.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;

/**
 * Created by WXF on 2014/7/21.
 */
public class SampleGloginreqSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;

    BufferedReader _sampleLogReader;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        InputStream in = getClass().getResourceAsStream("/loginreq_log.txt");
        _sampleLogReader = new BufferedReader(new InputStreamReader(in));

    }

    @Override
    public void nextTuple() {
        Utils.sleep(20000);

        String sampleLogLine = null;
        try{
            sampleLogLine = _sampleLogReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }

        String sentence = sampleLogLine.toString();

        _collector.emit(new Values(sentence));
    }

    @Override
    public void ack(Object id) {

    }

    @Override
    public void fail(Object id) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("gloginreq_log"));
    }
}
