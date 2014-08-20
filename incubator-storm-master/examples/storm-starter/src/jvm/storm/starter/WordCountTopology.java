/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter;

import storm.starter.spout.RandomSentenceSpout;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;


import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.io.FileNotFoundException;
//import java.util.ArrayList;

import storm.starter.util.Configuration;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {
    public static class SplitSentence extends ShellBolt implements IRichBolt {

        public SplitSentence() {
            super("python", "splitsentence.py");
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            return null;
        }
    }

    public static class WordCount extends BaseBasicBolt {
        Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null)
                count = 0;
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));

            System.out.println("截取字符：" + word + "  统计个数:" + count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws /*Exception*/AlreadyAliveException, InvalidTopologyException, InterruptedException, IOException {
        String topic = "test-replica-1";
        String zkRoot = "/tmp/testtmpabc";
        String spoutId = "test";

        BrokerHosts brokerHosts = new ZkHosts("172.29.201.208:2181,172.29.201.207:2181,172.29.201.205:2181");

        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, spoutId);

        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        //spoutConf.forceStartOffsetTime(-2);

        /*spoutConf.zkServers = new ArrayList<String>() {
            {
                add("nutch1");
            }
        };
        spoutConf.zkPort = 2181;*/

        Configuration rc = new Configuration("./config.properties");
        String intermediate_file = rc.getValue("intermediate_file");
        System.out.println(intermediate_file);

        File fh = new File(intermediate_file);
        boolean result = false;
        if(!fh.exists()){
            try {
                System.out.println("start to create the file!");
                result = fh.createNewFile();
                System.out.println("file create result:" + result);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        synchronized (fh) {
            FileWriter fw = new FileWriter(intermediate_file);
            fw.write("hallo world!!");
            fw.close();
        }


        TopologyBuilder builder = new TopologyBuilder();

        //builder.setSpout("spout", new KafkaSpout(spoutConf), 1);
        builder.setSpout("spout", new RandomSentenceSpout(), 1);

        builder.setBolt("split", new SplitSentence(), 1).shuffleGrouping("spout");
        builder.setBolt("count", new WordCount(), 1).fieldsGrouping("split", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        } else {
            conf.setMaxTaskParallelism(1);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, builder.createTopology());

            Thread.sleep(2000);

            cluster.shutdown();
        }
    }
}
