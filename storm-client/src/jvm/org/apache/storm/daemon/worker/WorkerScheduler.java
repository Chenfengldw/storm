package org.apache.storm.daemon.worker;
import org.apache.storm.utils.Utils;
import java.util.*;
import org.apache.storm.executor.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerScheduler {


    private static final Logger LOG = LoggerFactory.getLogger(WorkerScheduler.class);
    private int scheduleTime;
    private final List<Executor> executors;
    private boolean scheduling = false;
    private Map<String, Object> conf;

    public WorkerScheduler(Map<String, Object> conf, int time, List<Executor> execs){

        this.scheduleTime = time;
        this.executors = execs;
        this.conf = conf;

    }

    public void startScheduling(){

        this.scheduling = true;

        LOG.info("successfully start scheduling");



        Thread thread = new Thread(){
            public void run(){   
                while(true){
                    // sleep time interval
                    Utils.sleep(scheduleTime);

                    if(scheduling == false){
                        break;
                    }
                    int letGoExeIndex = getLongestQueueIndex(executors);
                    LOG.info("the longest queue is in executor with index {}", letGoExeIndex);
                    LOG.info("On this worker, executor num is {}", executors.size());
                    executors.get(letGoExeIndex).setScheduleFlag(true);
                    LOG.info("finish setting the target executor flag to true");
                    for(Executor e : executors){
                        if(e == executors.get(letGoExeIndex)){
                            LOG.info("this executor is the target one, do not need change");
                        }else{
                            LOG.info("schedule to stop the executor component is {}", e.getComponentId());
                            e.setScheduleFlag(false);
                        }
                    }
                }
            }
          };
        
        thread.start();


    }

    public void stopScheduling(){
        this.scheduling = false;
    }

    public int getLongestQueueIndex(List<Executor> execs){
        int longestIndex = 0;
        for(int i = 0; i < execs.size(); i++){
            int tmpQueueLength = execs.get(i).getReceiveQueue().getQueuedCount();
            int tmpMaxQueueLength = execs.get(longestIndex).getReceiveQueue().getQueuedCount();
            LOG.info("executor {} has queue length {}", i, tmpQueueLength);
            if(tmpQueueLength > tmpMaxQueueLength){
                LOG.info("executor {} has longer queue length {} than tmp max length {}", i, tmpQueueLength, tmpMaxQueueLength);
                longestIndex = i;
            }
        }
        return longestIndex;
    }

}