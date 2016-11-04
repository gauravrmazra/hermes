package pl.allegro.tech.hermes.consumers.supervisor.workload;

import pl.allegro.tech.hermes.api.Subscription;
import pl.allegro.tech.hermes.api.SubscriptionName;

import java.util.Optional;

public interface SubscriptionAssignmentAware {
    default void onSubscriptionAssigned(Subscription subscription) {}
    default void onAssignmentRemoved(SubscriptionName subscriptionName) {}
    Optional<String> watchedConsumerId();
}
