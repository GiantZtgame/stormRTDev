package storm.qule_game.spout;

import java.io.*;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.Map;

public class GRechargeSpout extends BaseRichSpout {
    BufferedReader _sampleLogReader;
    SpoutOutputCollector _collector;

    public void ack(Object msgId) {System.out.println("OK:"+msgId);}
    public void close() {}
    public void fail(Object msgId) {System.out.println("FAIL:"+msgId);}

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        InputStream in = getClass().getResourceAsStream("/gbill.txt");
        _sampleLogReader = new BufferedReader(new InputStreamReader(in));
        _collector = collector;
    }
    @Override
    public void nextTuple() {
        Utils.sleep(1000);

        String sampleLogLine = null;
        try{
            sampleLogLine = _sampleLogReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        String sentence = sampleLogLine.toString();
        _collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {declarer.declare(new Fields("line"));}
}