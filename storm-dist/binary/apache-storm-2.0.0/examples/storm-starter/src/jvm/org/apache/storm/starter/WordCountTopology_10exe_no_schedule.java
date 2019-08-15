/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.  The ASF licenses this file to you under the Apache License, Version
 * 2.0 (the "License"); you may not use this file except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */

package org.apache.storm.starter;

import java.util.HashMap;
import java.util.Map;
import org.apache.storm.utils.Utils;
import org.apache.storm.starter.spout.RandomSentenceSpout;
import org.apache.storm.starter.spout.GetSentenceFromFileSpout;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.ConfigurableTopology;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;

/**
 * This topology demonstrates Storm's stream groupings and multilang
 * capabilities.
 */
public class WordCountTopology_10exe_no_schedule {
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

      @Override
      public void execute(Tuple input) {
          super.execute(input);
          Utils.sleep(2);
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
        //Tuples emitted to BasicOutputCollector are automatically anchored to the input tuple,
        collector.emit(new Values(word, count));
      }
  
      @Override
      public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
      }
    }
  
    public static void main(String[] args) throws Exception {


      Config conf = new Config();
      conf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 2);

      if(args.length > 0){
        conf.put("schedule_time", args[0]);
        if(args.length > 1){
          conf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, Integer.parseInt(args[1]));
          if(args.length > 2){
            conf.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, Integer.parseInt(args[2]));
            if(args.length > 3){
              conf.put("sleep_time", Integer.parseInt(args[3]));
              if(args.length > 4){
                conf.put("exec_num", Integer.parseInt(args[4]));
              }else{
                conf.put("exec_num", 10);
              }
            }
            else{
              conf.put("sleep_time", 10);
            }
          }else{
            conf.put(Config.TOPOLOGY_TRANSFER_BATCH_SIZE, 128);
          }
        }else{
          conf.put(Config.TOPOLOGY_PRODUCER_BATCH_SIZE, 128);
        }
      }else{
        conf.put("schedule_time", "50");
      }
  
      TopologyBuilder builder = new TopologyBuilder();
  
      
      int exec_num = (int)conf.get("exec_num");

    
      builder.setSpout("spout", new GetSentenceFromFileSpout(),10);
      builder.setBolt("split", new SplitSentence(), exec_num).shuffleGrouping("spout");
      builder.setBolt("count", new WordCount(), 10).fieldsGrouping("split", new Fields("word"));
 
      //conf.put
      conf.setDebug(true);
  
      if (args != null && args.length > 0) {
        HashMap<String, String> assign_map = new HashMap<>();
        assign_map.put("spout", "supervisor-cpu02.maas");
        assign_map.put("split", "supervisor-storage05.maas");
        assign_map.put("count", "supervisor-cpu13.maas");
  
        conf.setNumWorkers(3);
        conf.setNumAckers(3);
        conf.put("assigned_flag", "1");
        conf.put("design_map", assign_map);

        String topoName = String.format("task%d_noschedule-sleep_time%s-batch_size%s",
                  exec_num,
                  conf.get("sleep_time"), 
                  conf.get(Config.TOPOLOGY_TRANSFER_BATCH_SIZE));
                  
        StormSubmitter.submitTopologyWithProgressBar(topoName, conf, builder.createTopology());
        //StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
      }
      else {
        // conf.setMaxTaskParallelism(3);
  
        // LocalCluster cluster = new LocalCluster();
        // cluster.submitTopology("word-count", conf, builder.createTopology());
  
        // Thread.sleep(10000);
  
        // cluster.shutdown();
      }
    }
  }
  
