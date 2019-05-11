package org.apache.storm.daemon.worker;

import java.util.List;
import org.apache.storm.executor.Executor;

public class WorkerScheduler {

    private int scheduleTime;
    private final List<Executor> executors;
    private boolean scheduling = false;

    public WorkerScheduler(int time, List<Executor> execs){

        this.scheduleTime = time;
        this.executors = execs;
    }

    public void startScheduling(){

        this.scheduling = true;

        while(true){
            int letGoExeIndex = getLongestQueueIndex(executors);
            executors.get(letGoExeIndex).setScheduleFlag(true);
            for(Executor e : executors){
                if(e == executors.get(letGoExeIndex)){
                    return;
                }else{
                    e.setScheduleFlag(false);
                }
            }

            if(scheduling == false){
                break;
            }
        }

    }

    public void stopScheduling(){
        this.scheduling = false;
    }

    public int getLongestQueueIndex(List<Executor> execs){
        int longestIndex = 0;
        for(int i = 0; i < execs.size(); i++){
            if(execs.get(i).getReceiveQueue().getQueuedCount() > execs.get(longestIndex).getReceiveQueue().getQueuedCount()){
                longestIndex = i;
            }
        }
        return longestIndex;
    }

}