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
package storm.mesos.schedulers;

import backtype.storm.scheduler.*;
import org.apache.mesos.Protos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.mesos.resources.AggregatedOffers;
import storm.mesos.resources.ResourceNotAvailableException;
import storm.mesos.util.MesosCommon;
import storm.mesos.util.RotatingMap;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Default Scheduler used by mesos-storm framework.
 */
public class DefaultScheduler implements IScheduler, IMesosStormScheduler {
  private final Logger log = LoggerFactory.getLogger(DefaultScheduler.class);
  private Map mesosStormConf;
  private final Map<String, MesosWorkerSlot> mesosWorkerSlotMap = new HashMap<>();

  @Override
  public void prepare(Map conf) {
    mesosStormConf = conf;
  }

  private List<MesosWorkerSlot> getMesosWorkerSlots(Map<String, AggregatedOffers> aggregatedOffersPerNode,
                                                    Collection<String> nodesWithExistingSupervisors,
                                                    TopologyDetails topologyDetails,
                                                    int slotsNeeded) {

    double requestedWorkerCpu = MesosCommon.topologyWorkerCpu(mesosStormConf, topologyDetails);
    double requestedWorkerMem = MesosCommon.topologyWorkerMem(mesosStormConf, topologyDetails);

    List<MesosWorkerSlot> mesosWorkerSlots = new ArrayList<>();
    boolean slotFound = false;

    do {
      slotFound = false;
      for (String currentNode : aggregatedOffersPerNode.keySet()) {
        AggregatedOffers aggregatedOffers = aggregatedOffersPerNode.get(currentNode);

        boolean supervisorExists = nodesWithExistingSupervisors.contains(currentNode);

        if (!aggregatedOffers.isFit(mesosStormConf, topologyDetails, supervisorExists)) {
          log.info("{} is not a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}",
                   aggregatedOffers.toString(), topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
          continue;
        }

        log.info("{} is a fit for {} requestedWorkerCpu: {} requestedWorkerMem: {}", aggregatedOffers.toString(),
                 topologyDetails.getId(), requestedWorkerCpu, requestedWorkerMem);
        MesosWorkerSlot mesosWorkerSlot;
        try {
          mesosWorkerSlot = SchedulerUtils.createMesosWorkerSlot(mesosStormConf, aggregatedOffers, topologyDetails, supervisorExists);
        } catch (ResourceNotAvailableException rexp) {
          log.warn(rexp.getMessage());
          continue;
        }

        nodesWithExistingSupervisors.add(currentNode);
        mesosWorkerSlots.add(mesosWorkerSlot);
        slotFound = true;
        if (--slotsNeeded == 0) {
          break;
        }
      }
    } while (slotFound && slotsNeeded > 0);

    return mesosWorkerSlots;
  }
  /*
   * Different topologies have different resource requirements in terms of cpu and memory. So when Mesos asks
   * this scheduler for a list of available worker slots, we create "MesosWorkerSlot" and store them into mesosWorkerSlotMap.
   * Notably, we return a list of MesosWorkerSlot objects, even though Storm is only aware of the WorkerSlot type.  However,
   * since a MesosWorkerSlot *is* a WorkerSlot (in the polymorphic sense), Storm treats the list as WorkerSlot objects.
   *
   * Note:
   * 1. "MesosWorkerSlot" is the same as WorkerSlot except that it is dedicated for a topology upon creation. This means that,
   *    a MesosWorkerSlot belonging to one topology cannot be used to launch a worker belonging to a different topology.
   * 2. Please note that this method is called before schedule is invoked. We use this opportunity to assign the MesosWorkerSlot
   *    to a specific topology and store the state in "mesosWorkerSlotMap". This way, when Storm later calls schedule, we can just
   *    look up the "mesosWorkerSlotMap" for a list of available slots for the particular topology.
   * 3. Given MesosWorkerSlot extends WorkerSlot, we shouldn't have to really create a "mesosWorkerSlotMap". Instead, in the schedule
   *    method, we could have just upcasted the "WorkerSlot" to "MesosWorkerSlot". But this is not currently possible because storm
   *    passes a recreated version of WorkerSlot to schedule method instead of passing the WorkerSlot returned by this method as is.
    */
  @Override
  public List<WorkerSlot> allSlotsAvailableForScheduling(RotatingMap<Protos.OfferID, Protos.Offer> offers,
                                                         Collection<SupervisorDetails> existingSupervisors,
                                                         Topologies topologies, Set<String> topologiesMissingAssignments) {
    if (topologiesMissingAssignments.isEmpty()) {
      log.info("Declining all offers that are currently buffered because no topologies need assignments");
      // TODO(ksoundararaj): Do we need to clear offers not that consolidate resources?
      offers.clear();
      return new ArrayList<>();
    }

    log.info("Topologies that need assignments: {}", topologiesMissingAssignments.toString());

    List<WorkerSlot> allSlots = new ArrayList<>();
    Map<String, AggregatedOffers> aggregatedOffersPerNode = MesosCommon.getAggregatedOffersPerNode(offers);

    for (String currentTopology : topologiesMissingAssignments) {
      TopologyDetails topologyDetails = topologies.getById(currentTopology);
      // TODO: remove this hack later
      // int slotsNeeded = topologyDetails.getNumWorkers();
      int slotsNeeded = MesosCommon.getMaxSlotsForTopology(topologyDetails);

      log.info("Trying to find {} slots for {}", slotsNeeded, topologyDetails.getId());
      if (slotsNeeded <= 0) {
        continue;
      }

      Set<String> nodesWithExistingSupervisors = new HashSet<>();
      for (String currentNode : aggregatedOffersPerNode.keySet()) {
        if (SchedulerUtils.supervisorExists(currentNode, existingSupervisors, currentTopology)) {
          nodesWithExistingSupervisors.add(currentNode);
        }
      }

      List<MesosWorkerSlot> mesosWorkerSlotList = getMesosWorkerSlots(aggregatedOffersPerNode, nodesWithExistingSupervisors, topologyDetails, slotsNeeded);
      for (MesosWorkerSlot mesosWorkerSlot : mesosWorkerSlotList) {
        String slotId = String.format("%s:%s", mesosWorkerSlot.getNodeId(), mesosWorkerSlot.getPort());
        mesosWorkerSlotMap.put(slotId, mesosWorkerSlot);
        allSlots.add(mesosWorkerSlot);
      }

      log.info("Number of available slots for {} : {}", topologyDetails.getId(), mesosWorkerSlotList.size());
    }

    log.info("Number of available slots: {}", allSlots.size());
    log.info("all slots:");
    for (WorkerSlot slot : allSlots) {
      log.info(" - {}", slot.toString());
    }
    return allSlots;
  }


  Map<String, List<MesosWorkerSlot>> getMesosWorkerSlotPerTopology(List<WorkerSlot> workerSlots) {
    HashMap<String, List<MesosWorkerSlot>> perTopologySlotList = new HashMap<>();

    for (WorkerSlot workerSlot : workerSlots) {
      if (workerSlot.getNodeId() == null) {
        log.warn("Unexpected: Node id is null for worker slot while scheduling");
        continue;
      }
      MesosWorkerSlot mesosWorkerSlot = mesosWorkerSlotMap.get(String.format("%s:%d",
                                                                             workerSlot.getNodeId(),
                                                                             workerSlot.getPort()));

      String topologyId = mesosWorkerSlot.getTopologyId();
      if (perTopologySlotList.get(topologyId) == null) {
        perTopologySlotList.put(topologyId, new ArrayList<MesosWorkerSlot>());
      }
      perTopologySlotList.get(topologyId).add(mesosWorkerSlot);
    }

    return  perTopologySlotList;
  }

  List<List<ExecutorDetails>> executorsPerWorkerList(Cluster cluster, TopologyDetails topologyDetails,
                                                     int slotsRequested, int slotsAssigned, int slotsAvailable) {
    List<List<ExecutorDetails>> executorsPerWorkerList = new ArrayList<>();
    Collection<ExecutorDetails> executors = cluster.getUnassignedExecutors(topologyDetails);

    // Check if we don't actually need to schedule any executors because all requested slots are assigned already.
    if (slotsRequested == slotsAssigned) {
      if (executors.isEmpty()) {
        // TODO: print executors list cleanly in a single line
        String msg = String.format("executorsPerWorkerList - slotsRequested: %d == slotsAssigned: %d, BUT there are unassigned executors which is nonsensical",
                                   slotsRequested, slotsAssigned);
        log.error(msg);
        throw new RuntimeException(msg);
      }
      // TODO: switch from info to debug
      log.info("executorsPerWorkerList - slotsRequested: {} == slotsAssigned: {}, so no need to schedule any executors", slotsRequested, slotsAssigned);
      return executorsPerWorkerList;
    }

    // If there are not any unassigned executors, we need to re-distribute all currently existing executors across workers
    if (executors.isEmpty()) {
      log.info("There are currently no unassigned executors. Using all currently existing executors instead.");
      SchedulerAssignment schedulerAssignment = cluster.getAssignmentById(topologyDetails.getId());
      // Get all currently existing executors
      executors = schedulerAssignment.getExecutors();
      // Un-assign them
      cluster.freeSlots(schedulerAssignment.getSlots());
      log.info("executorsPerWorkerList - slotsAvailable: {}, slotsAssigned: {}, slotsFreed: {}", slotsAvailable, slotsAssigned, schedulerAssignment.getSlots().size());
      slotsAvailable += slotsAssigned;
    }

    for (ExecutorDetails exec : executors) {
      log.info("executorsPerWorkerList - available executor: {}", exec.toString());
    }
    // log.info("executorsPerWorkerList - adding {} empty lists to executorsPerWorkerList", slotsAvailable);
    for (int i = 0; i < slotsAvailable; i++) {
      executorsPerWorkerList.add(new ArrayList<ExecutorDetails>());
    }

    List<ExecutorDetails> executorList = new ArrayList<>(executors);

    /* The goal of this scheduler is to mimic Storm's default version. Storm's default scheduler sorts the
     * executors by their id before spreading them across the available workers.
     */
    Collections.sort(executorList, new Comparator<ExecutorDetails>() {
      public int compare(ExecutorDetails e1, ExecutorDetails e2) {
        return e1.getStartTask() - e2.getStartTask();
      }
    });

    int index = -1;
    for (ExecutorDetails executorDetails : executorList) {
      index = ++index % slotsAvailable;
      // log.info("executorsPerWorkerList -- adding {} to list at index {}", executorDetails.toString(), index);
      executorsPerWorkerList.get(index).add(executorDetails);
    }

    return executorsPerWorkerList;
  }

  /**
   * Schedule function looks in the "mesosWorkerSlotMap" to determine which topology owns the particular
   * WorkerSlot and assigns the executors accordingly.
   */
  @Override
  public void schedule(Topologies topologies, Cluster cluster) {
    List<WorkerSlot> workerSlots = cluster.getAvailableSlots();
    log.info("Scheduling the following worker slots from cluster.getAvailableSlots:");
    for (WorkerSlot ws : workerSlots) {
      log.info("- {}", ws.toString());
    }
    Map<String, List<MesosWorkerSlot>> perTopologySlotList = getMesosWorkerSlotPerTopology(workerSlots);
    log.info("Schedule the per-topology slots:");
    for (String topo : perTopologySlotList.keySet()) {
      log.info("- {}", topo);
      for (MesosWorkerSlot mws : perTopologySlotList.get(topo)) {
        log.info("-- {}", mws.toString());
      }
    }

    // So far we know how many MesosSlots each of the topologies have got. Let's assign executors for each of them
    for (String topologyId : perTopologySlotList.keySet()) {
      TopologyDetails topologyDetails = topologies.getById(topologyId);
      List<MesosWorkerSlot> mesosWorkerSlots = perTopologySlotList.get(topologyId);

      // TODO: remove this dead-code later
      // int slotsRequested = MesosCommon.getMaxSlotsForTopology(topologyDetails);
      int slotsRequested = topologyDetails.getNumWorkers();
      int slotsAssigned = cluster.getAssignedNumWorkers(topologyDetails);

      if (mesosWorkerSlots.size() == 0) {
        log.warn("No slots found for topology {} while scheduling", topologyId);
        continue;
      }

      int slotsAvailable = Math.min(mesosWorkerSlots.size(), (slotsRequested - slotsAssigned));
      log.info("topologyId: {}, slotsRequested: {}, slotsAssigned: {}, slotsAvailable: {}", topologyId, slotsRequested, slotsAssigned, slotsAvailable);

      List<List<ExecutorDetails>> executorsPerWorkerList = executorsPerWorkerList(cluster, topologyDetails, slotsRequested, slotsAssigned, slotsAvailable);

      for (int i = 0; i < slotsAvailable; i++) {
        log.info("schedule: mesosworkerSlot: {}, topologyId: {}, executorsPerWorkerList: {}", mesosWorkerSlots.get(0).toString(), topologyId, executorsPerWorkerList.get(0).toString());
        cluster.assign(mesosWorkerSlots.remove(0), topologyId, executorsPerWorkerList.remove(0));
      }
    }
    mesosWorkerSlotMap.clear();
  }
}
