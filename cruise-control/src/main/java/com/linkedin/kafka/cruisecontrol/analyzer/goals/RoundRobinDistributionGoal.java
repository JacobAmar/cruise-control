/*
 * Copyright 2017 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.analyzer.goals;

import com.linkedin.kafka.cruisecontrol.analyzer.ActionAcceptance;
import com.linkedin.kafka.cruisecontrol.analyzer.ActionType;
import com.linkedin.kafka.cruisecontrol.analyzer.BalancingAction;
import com.linkedin.kafka.cruisecontrol.analyzer.OptimizationOptions;
import com.linkedin.kafka.cruisecontrol.common.Statistic;
import com.linkedin.kafka.cruisecontrol.exception.OptimizationFailureException;
import com.linkedin.kafka.cruisecontrol.model.Broker;
import com.linkedin.kafka.cruisecontrol.model.ClusterModel;
import com.linkedin.kafka.cruisecontrol.model.ClusterModelStats;
import com.linkedin.kafka.cruisecontrol.model.Replica;
import com.linkedin.kafka.cruisecontrol.monitor.ModelCompletenessRequirements;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A goal that distributes replicas evenly across the cluster in a round-robin fashion.
 */
public class RoundRobinDistributionGoal extends ReplicaDistributionAbstractGoal {
    private static final Logger LOG = LoggerFactory.getLogger(RoundRobinDistributionGoal.class);
    private int _numIterations;
    private static final int MAX_ITERATIONS = 10;

    public RoundRobinDistributionGoal() {
        super();
    }

    @Override
    public ModelCompletenessRequirements clusterModelCompletenessRequirements() {
        return new ModelCompletenessRequirements(1, 0.0, true);
    }

    @Override
    public String name() {
        return RoundRobinDistributionGoal.class.getSimpleName();
    }

    @Override
    public boolean isHardGoal() {
        return false;
    }

    @Override
    protected void initGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        super.initGoalState(clusterModel, optimizationOptions);
        _numIterations = 0;
        
        if (clusterModel.aliveBrokers().size() < 2) {
            throw new OptimizationFailureException("At least 2 alive brokers are needed for round-robin distribution.");
        }
    }

    @Override
    protected void updateGoalState(ClusterModel clusterModel, OptimizationOptions optimizationOptions)
            throws OptimizationFailureException {
        _numIterations++;
        if (_numIterations >= MAX_ITERATIONS) {
            _finished = true;
        }
        super.updateGoalState(clusterModel, optimizationOptions);
    }

    @Override
    protected void rebalanceForBroker(Broker broker,
                                     ClusterModel clusterModel,
                                     Set<Goal> optimizedGoals,
                                     OptimizationOptions optimizationOptions) 
            throws OptimizationFailureException {
        
        LOG.debug("Rebalancing broker {} to achieve round-robin distribution", broker.id());
        
        if (!broker.isAlive()) {
            LOG.debug("Broker {} is not alive, skipping", broker.id());
            return;
        }

        List<Broker> healthyBrokers = new ArrayList<>(clusterModel.aliveBrokers());
        Collections.sort(healthyBrokers, Comparator.comparingInt(Broker::id));

        distributeLeaderReplicas(broker, clusterModel, healthyBrokers, optimizedGoals, optimizationOptions);
        distributeAllReplicas(broker, clusterModel, healthyBrokers, optimizedGoals, optimizationOptions);
    }

    private void distributeLeaderReplicas(Broker broker,
                                        ClusterModel clusterModel,
                                        List<Broker> healthyBrokers,
                                        Set<Goal> optimizedGoals,
                                        OptimizationOptions optimizationOptions) {
        List<Replica> leaderReplicas = new ArrayList<>(broker.leaderReplicas());
        Collections.sort(leaderReplicas, Comparator.comparing(r -> r.topicPartition().toString()));

        for (Replica leader : leaderReplicas) {
            if (shouldSkipReplica(leader, optimizationOptions)) {
                continue;
            }

            Broker targetBroker = findBestTargetBroker(leader, healthyBrokers, clusterModel);
            if (targetBroker != null) {
                maybeApplyBalancingAction(clusterModel, leader, Collections.singletonList(targetBroker),
                                        ActionType.LEADERSHIP_MOVEMENT, optimizedGoals, optimizationOptions);
            }
        }
    }

    private void distributeAllReplicas(Broker broker,
                                     ClusterModel clusterModel,
                                     List<Broker> healthyBrokers,
                                     Set<Goal> optimizedGoals,
                                     OptimizationOptions optimizationOptions) {
        List<Replica> replicas = new ArrayList<>(broker.replicas());
        Collections.sort(replicas, (r1, r2) -> {
            int sizeComparison = Integer.compare(r1.broker().replicas().size(), r2.broker().replicas().size());
            return sizeComparison != 0 ? sizeComparison 
                                     : r1.topicPartition().toString().compareTo(r2.topicPartition().toString());
        });

        for (Replica replica : replicas) {
            if (shouldSkipReplica(replica, optimizationOptions)) {
                continue;
            }

            Broker targetBroker = findBestTargetBroker(replica, healthyBrokers, clusterModel);
            if (targetBroker != null) {
                maybeApplyBalancingAction(clusterModel, replica, Collections.singletonList(targetBroker),
                                        ActionType.INTER_BROKER_REPLICA_MOVEMENT, optimizedGoals, optimizationOptions);
            }
        }
    }

    private boolean shouldSkipReplica(Replica replica, OptimizationOptions optimizationOptions) {
        if (optimizationOptions.excludedTopics().contains(replica.topicPartition().topic())) {
            return true;
        }
        
        double currentCount = replica.broker().replicas().size();
        double meanCount = _avgReplicasOnAliveBroker;
        double balanceLimit = meanCount * (balancePercentage() - 1);
        return Math.abs(currentCount - meanCount) <= balanceLimit;
    }

    private Broker findBestTargetBroker(Replica replica, List<Broker> healthyBrokers, ClusterModel clusterModel) {
        return healthyBrokers.stream()
            .filter(b -> b.id() != replica.broker().id())
            .filter(b -> !b.replicas().contains(replica))
            .filter(b -> !clusterModel.partition(replica.topicPartition()).replicas().stream()
                         .map(Replica::broker)
                         .map(Broker::id)
                         .collect(Collectors.toSet())
                         .contains(b.id()))
            .min(Comparator.comparingInt(b -> b.replicas().size()))
            .orElse(null);
    }

    @Override
    protected boolean selfSatisfied(ClusterModel clusterModel, BalancingAction action) {
        if (!isValidAction(action, clusterModel)) {
            return false;
        }
        
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
        
        int sourceCount = sourceBroker.replicas().size();
        int destCount = destinationBroker.replicas().size();
        
        return Math.abs((sourceCount - 1) - destCount) < Math.abs(sourceCount - destCount);
    }

    private boolean isValidAction(BalancingAction action, ClusterModel clusterModel) {
        if (action == null) {
            return false;
        }
        
        Broker sourceBroker = clusterModel.broker(action.sourceBrokerId());
        Broker destinationBroker = clusterModel.broker(action.destinationBrokerId());
        
        if (sourceBroker == null || destinationBroker == null) {
            return false;
        }

        return clusterModel.partition(action.topicPartition()) != null;
    }

    @Override
    public ClusterModelStatsComparator clusterModelStatsComparator() {
        return new ClusterModelStatsComparator() {
            @Override
            public int compare(ClusterModelStats stats1, ClusterModelStats stats2) {
                double stdDev1 = stats1.replicaStats().get(Statistic.ST_DEV).doubleValue();
                double stdDev2 = stats2.replicaStats().get(Statistic.ST_DEV).doubleValue();
                return Double.compare(stdDev1, stdDev2);
            }

            @Override
            public String explainLastComparison() {
                return "Comparison based on replica distribution standard deviation";
            }
        };
    }

    @Override
    public ActionAcceptance actionAcceptance(BalancingAction action, ClusterModel clusterModel) {
        return ActionAcceptance.ACCEPT;
    }

    @Override
    protected int numInterestedReplicas(ClusterModel clusterModel) {
        return clusterModel.numReplicas();
    }

    @Override
    protected double balancePercentage() {
        return 1.05;
    }
}
