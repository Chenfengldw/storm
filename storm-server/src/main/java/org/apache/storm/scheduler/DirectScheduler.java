package org.apache.storm.scheduler;

import org.apache.storm.scheduler.*;
//import clojure.lang.PersistentArrayMap;
import java.util.*;

public class DirectScheduler implements IScheduler{


    private Map conf;

    @Override
    public void prepare(Map conf){
        this.conf = conf;
    }   

    @Override
    public void schedule(Topologies topologies, Cluster cluster) {
        System.out.println("DirectScheduler: begin scheduling");
        // Gets the topology which we want to schedule
        Collection<TopologyDetails> topologyDetailes;
        TopologyDetails topology;
        //whether to allocate tasks
        String assignedFlag;
        Map map;
        Iterator<String> iterator = null;

        topologyDetailes = topologies.getTopologies();
        for(TopologyDetails td: topologyDetailes){
            map = td.getConf();
            assignedFlag = (String)map.get("assigned_flag");

            //flag = "1" means need scheduling
            if(assignedFlag != null && assignedFlag.equals("1")){
                System.out.println("finding topology named " + td.getName());
                topologyAssign(cluster, td, map);
            }else {
                System.out.println("topology assigned is null");
            }
        }

    
        //new EvenScheduler().schedule(topologies, cluster);
    }

    @Override
    public Map config(){
        return conf;
    }

    /**
     * topo scheduling
     * @param cluster
     * cluster info
     * @param topology
     * policy to shceduling
     * @param map
     * map config
     */
    private void topologyAssign(Cluster cluster, TopologyDetails topology, Map map){
        Set<String> keys;
        Map designMap;
        Iterator<String> iterator;

        iterator = null;
        // make sure the special topology is submitted,
        if (topology != null) {
            designMap = (Map)map.get("design_map");//.get("design_map");
            if(designMap != null){
                System.out.println("design map size is " + designMap.size());
                keys = designMap.keySet();
                iterator = keys.iterator();

                System.out.println("keys size is " + keys.size());
            }

            if(designMap == null || designMap.size() == 0){
                System.out.println("design map is null");
            }

            boolean needsScheduling = cluster.needsScheduling(topology);

            if (!needsScheduling) {
                System.out.println(String.format("topology  %s finished scheduling",  topology.getName()));
            } else {
                System.out.println("Our special topology needs scheduling.");
                // find out all the needs-scheduling components of this topology
                Map<String, List<ExecutorDetails>> componentToExecutors = cluster.getNeedsSchedulingComponentToExecutors(topology);

                //System.out.println("needs scheduling(component->executor): " + componentToExecutors);
                System.out.println("needs scheduling(executor->components): " + cluster.getNeedsSchedulingExecutorToComponents(topology));
                SchedulerAssignment currentAssignment = cluster.getAssignmentById(topology.getId());
                if (currentAssignment != null) {
                    System.out.println("current assignments: " + currentAssignment.getExecutorToSlot());
                } else {
                    System.out.println("current assignments: {}");
                }

                String componentName;
                String nodeName;

                Set<String> usedSlots = new HashSet<String>();
                
                if(designMap != null && iterator != null){
                    while (iterator.hasNext()){
                        componentName = iterator.next();
 
                        // System.out.println(componentName);
                        // System.out.println(designMap.get(componentName));
                        //nodeName = (String)designMap.get(componentName);
                        System.out.println("now scheduling component->node:" + componentName + "->" + designMap.get(componentName));
                
                        componentAssign(cluster, topology, componentToExecutors, componentName.toString(), designMap.get(componentName).toString());
                    }
                }
            }
        }
    }

    /**
     * component scheduler
     * @param cluster
     * info of clusters
     * @param topology
     * topology for scheduling
     * @param totalExecutors
     * component for scheduling
     * @param componentName
     * name of components
     * @param supervisorName
     * name of nodes
     */
    private void componentAssign(Cluster cluster, TopologyDetails topology, Map<String, List<ExecutorDetails>> componentToExecutors, String componentName, String supervisorName){
        if (!componentToExecutors.containsKey(componentName)) {
            System.out.println(String.format("%s does not need scheduling.", componentName));
        } else {
            System.out.println(String.format("%s need scheduling.", componentName));
            List<ExecutorDetails> executors = componentToExecutors.get(componentName);

            // find out the our "special-supervisor" from the supervisor metadata
            Collection<SupervisorDetails> supervisors = cluster.getSupervisors().values();
            SupervisorDetails specialSupervisor = null;
            for (SupervisorDetails supervisor : supervisors) {
                Map meta = (Map) supervisor.getSchedulerMeta();

                if(meta != null && meta.get("name") != null){
                    System.out.println("supervisor name:" + meta.get("name"));

                    if (meta.get("name").equals(supervisorName)) {
                        System.out.println("Supervisor finding");
                        specialSupervisor = supervisor;
                        break;
                    }
                }else {
                    System.out.println("Can't find the target supervisor");
                }

            }

            // found the special supervisor
            if (specialSupervisor != null) {
                System.out.println("Found the special-supervisor");
                List<WorkerSlot> availableSlots = cluster.getAvailableSlots(specialSupervisor);
                int slotNumber = availableSlots.size();

                // release slots if not enough
                if (availableSlots.isEmpty() && !executors.isEmpty()) {
                    for (Integer port : cluster.getUsedPorts(specialSupervisor)) {
                        cluster.freeSlot(new WorkerSlot(specialSupervisor.getId(), port));
                    }
                }

                // get available slot
                availableSlots = cluster.getAvailableSlots(specialSupervisor);
                int slotIndex = 0;
                cluster.assign(availableSlots.get(0), topology.getId(), executors);
                // for(ExecutorDetails exe : executors){
                //     List<ExecutorDetails> tmpList = new ArrayList<ExecutorDetails>();
                //     tmpList.add(exe);
                //     cluster.assign(availableSlots.get(0), topology.getId(), tmpList);
                //     tmpList.remove(0);
                //     slotIndex = (slotIndex+1)%slotNumber;
                // }
                //cluster.assign(availableSlots.get(slotIndex), topology.getId(), executors);
                //System.out.println("We have available slot:");
                System.out.println("We assigned executors:" + executors + " to slot: [" + availableSlots.get(slotIndex).getNodeId() + ", " + availableSlots.get(0).getPort() + "]");
            } else {
                System.out.println("There is no supervisor find!!!");
            }
        }
    }
}
