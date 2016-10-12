package pl.allegro.tech.hermes.consumers.supervisor.workload;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;
import pl.allegro.tech.hermes.common.exception.InternalProcessingException;
import pl.allegro.tech.hermes.consumers.subscription.cache.SubscriptionsCache;

import java.util.Optional;

public class SubscriptionAssignmentRegistry implements SubscriptionAssignmentAware {

    private final CuratorFramework curator;

    private final SubscriptionAssignmentPathSerializer pathSerializer;

    private final SubscriptionAssignmentCache subscriptionAssignmentCache;

    public SubscriptionAssignmentRegistry(CuratorFramework curator,
                                          String path,
                                          SubscriptionsCache subscriptionsCache,
                                          SubscriptionAssignmentPathSerializer pathSerializer) {
        this.curator = curator;
        this.pathSerializer = pathSerializer;
        this.subscriptionAssignmentCache =
                new SubscriptionAssignmentCache(curator, path, subscriptionsCache, pathSerializer);
    }

    public void start() throws Exception {
        subscriptionAssignmentCache.registerAssignementCallback(this);
        subscriptionAssignmentCache.start();
    }

    public void stop() throws Exception {
        subscriptionAssignmentCache.stop();
    }

    @Override
    public void onSubscriptionAssigned(Subscription subscription) {}

    @Override
    public void onAssignmentRemoved(SubscriptionName subscriptionName) {
        removeSubscriptionEntryIfEmpty(subscriptionName);
    }

    @Override
    public Optional<String> watchedConsumerId() {
        return Optional.empty();
    }

    public void registerAssignementCallback(SubscriptionAssignmentAware callback) {
        subscriptionAssignmentCache.registerAssignementCallback(callback);
    }

    public boolean isAssignedTo(String nodeId, SubscriptionName subscription) {
        return subscriptionAssignmentCache.isAssignedTo(nodeId, subscription);
    }

    public SubscriptionAssignmentView createSnapshot() {
        return subscriptionAssignmentCache.createSnapshot();
    }

    private void removeSubscriptionEntryIfEmpty(SubscriptionName subscriptionName) {
        askCuratorPolitely(() -> {
            if (curator.getChildren().forPath(pathSerializer.serialize(subscriptionName)).isEmpty()) {
                curator.delete().guaranteed().forPath(pathSerializer.serialize(subscriptionName));
            }
        });
    }

    public void dropAssignment(SubscriptionAssignment assignment) {
        askCuratorPolitely(() -> curator.delete().guaranteed()
                .forPath(pathSerializer.serialize(assignment.getSubscriptionName(), assignment.getConsumerNodeId())));
    }

    public void addPersistentAssignment(SubscriptionAssignment assignment) {
        addAssignment(assignment, CreateMode.PERSISTENT);
    }

    public void addEphemeralAssignment(SubscriptionAssignment assignment) {
        addAssignment(assignment, CreateMode.EPHEMERAL);
    }

    private void addAssignment(SubscriptionAssignment assignment, CreateMode createMode) {
        askCuratorPolitely(() -> curator.create().creatingParentsIfNeeded().withMode(createMode)
                .forPath(pathSerializer.serialize(assignment.getSubscriptionName(), assignment.getConsumerNodeId())));
    }

    interface CuratorTask {
        void run() throws Exception;
    }

    private void askCuratorPolitely(CuratorTask task) {
        try {
            task.run();
        } catch (KeeperException.NodeExistsException | KeeperException.NoNodeException ex) {
            // ignore
        } catch (Exception ex) {
            throw new InternalProcessingException(ex);
        }
    }
}
