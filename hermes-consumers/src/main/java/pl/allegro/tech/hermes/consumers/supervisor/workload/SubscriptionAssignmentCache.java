package pl.allegro.tech.hermes.consumers.supervisor.workload;

import org.apache.curator.framework.CuratorFramework;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;
import pl.allegro.tech.hermes.infrastructure.zookeeper.cache.HierarchicalCache;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

public class SubscriptionAssignmentCache {

    private static final int SUBSCRIPTION_LEVEL = 0;

    private static final int ASSIGNMENT_LEVEL = 1;

    private final Set<SubscriptionAssignment> assignments = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private final HierarchicalCache cache;

    private final SubscriptionsCache subscriptionsCache;

    private final SubscriptionAssignmentPathSerializer pathSerializer;

    public SubscriptionAssignmentCache(CuratorFramework curator, String path, SubscriptionsCache subscriptionsCache,
                                       SubscriptionAssignmentPathSerializer pathSerializer) {
        this.subscriptionsCache = subscriptionsCache;
        this.pathSerializer = pathSerializer;
        this.cache = new HierarchicalCache(
                curator, Executors.newSingleThreadScheduledExecutor(), path, 2, Collections.emptyList()
        );

        cache.registerCallback(ASSIGNMENT_LEVEL, (e) -> {
            SubscriptionAssignment assignment = pathSerializer.deserialize(e.getData().getPath());
            switch (e.getType()) {
                case CHILD_ADDED:
                    assignments.add(assignment);
                    break;
                case CHILD_REMOVED:
                    assignments.remove(assignment);
                    break;
            }
        });
    }

    public void start() throws Exception {
        cache.start();
    }

    public void stop() throws Exception {
        cache.stop();
    }

    public void registerAssignementCallback(SubscriptionAssignmentAware callback) {
        cache.registerCallback(ASSIGNMENT_LEVEL, (e) -> {
            SubscriptionAssignment assignment = pathSerializer.deserialize(e.getData().getPath());
            if (!callback.watchedConsumerId().isPresent()
                    || callback.watchedConsumerId().get().equals(assignment.getConsumerNodeId())) {
                switch (e.getType()) {
                    case CHILD_ADDED:
                        callback.onSubscriptionAssigned(
                                subscriptionsCache.getSubscription(assignment.getSubscriptionName()));
                        break;
                    case CHILD_REMOVED:
                        callback.onAssignmentRemoved(assignment.getSubscriptionName());
                        break;
                }
            }
        });
    }

    public boolean isAssignedTo(String nodeId, SubscriptionName subscription) {
        return assignments.stream()
                .anyMatch(a -> a.getSubscriptionName().equals(subscription) && a.getConsumerNodeId().equals(nodeId));
    }

    public SubscriptionAssignmentView createSnapshot() {
        return SubscriptionAssignmentView.of(assignments);
    }
}
