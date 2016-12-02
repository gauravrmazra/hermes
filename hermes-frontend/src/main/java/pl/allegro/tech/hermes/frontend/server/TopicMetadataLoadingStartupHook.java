package pl.allegro.tech.hermes.frontend.server;

import org.glassfish.hk2.api.ServiceLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.config.Configs;
import pl.allegro.tech.hermes.common.hook.Hook;
import pl.allegro.tech.hermes.common.hook.ServiceAwareHook;
import pl.allegro.tech.hermes.domain.group.GroupRepository;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache;
import pl.allegro.tech.hermes.frontend.metric.CachedTopic;
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer;

import javax.inject.Inject;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;

public class TopicMetadataLoadingStartupHook implements ServiceAwareHook {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataLoadingStartupHook.class);

    private final BrokerMessageProducer brokerMessageProducer;

    private final TopicsCache topicsCache;

    private final GroupRepository groupRepository;

    private final TopicRepository topicRepository;

    private final int retryCount;

    private final long retryInterval;

    @Inject
    public TopicMetadataLoadingStartupHook(BrokerMessageProducer brokerMessageProducer,
                                           TopicsCache topicsCache,
                                           GroupRepository groupRepository,
                                           TopicRepository topicRepository,
                                           ConfigFactory config) {
        this(brokerMessageProducer,
                topicsCache,
                groupRepository,
                topicRepository,
                config.getIntProperty(Configs.FRONTEND_STARTUP_TOPIC_METADATA_LOADING_RETRY_COUNT),
                config.getLongProperty(Configs.FRONTEND_STARTUP_TOPIC_METADATA_LOADING_RETRY_INTERVAL));
    }

    public TopicMetadataLoadingStartupHook(BrokerMessageProducer brokerMessageProducer,
                                           TopicsCache topicsCache,
                                           GroupRepository groupRepository,
                                           TopicRepository topicRepository,
                                           int retryCount,
                                           long retryInterval) {
        this.brokerMessageProducer = brokerMessageProducer;
        this.topicsCache = topicsCache;
        this.groupRepository = groupRepository;
        this.topicRepository = topicRepository;
        this.retryCount = retryCount;
        this.retryInterval = retryInterval;
    }

    @Override
    public void accept(ServiceLocator serviceLocator) {
        logger.info("Loading topics metadata");
        List<TopicName> topicsLeft = getTopics();
        int initialTopicsCount = topicsLeft.size();
        int retries = retryCount + 1;
        while (retries > 0) {
            retries--;
            topicsLeft = topicsLeft.stream().filter(this::topicMetadataNotAvailable).collect(toList());
            if (!topicsLeft.isEmpty() && retries > 0) {
                waitBeforeNextRetry();
            }
        }
        logResultInfo(topicsLeft, initialTopicsCount);
    }

    private List<TopicName> getTopics() {
        return groupRepository.listGroupNames().stream()
                .flatMap(group -> topicRepository.listTopicNames(group).stream().map(topic -> new TopicName(group, topic)))
                .collect(toList());
    }

    private boolean topicMetadataNotAvailable(TopicName topic) {
        return !topicMetadataAvailable(topic);
    }

    private boolean topicMetadataAvailable(TopicName topic) {
        Optional<CachedTopic> cachedTopic = topicsCache.getTopic(topic.qualifiedName());
        if (cachedTopic.isPresent()) {
            if (brokerMessageProducer.isTopicAvailable(cachedTopic.get())) {
                logger.info("Topic {} metadata available", topic.qualifiedName());
                return true;
            } else {
                logger.warn("Topic {} metadata not available yet", topic.qualifiedName());
            }
        } else {
            logger.warn("Could not load topic {} metadata, topic not found in cache", topic.qualifiedName());
        }
        return false;
    }

    private void waitBeforeNextRetry() {
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
            logger.warn("Waiting for broker topics availability interrupted.");
        }
    }

    private void logResultInfo(List<TopicName> topicsLeft, int initialTopicsCount) {
        int successfulTopicsCount = initialTopicsCount - topicsLeft.size();
        if (successfulTopicsCount > 0) {
            logger.info("Successfully loaded metadata for {} topics", successfulTopicsCount);
        }
        if (topicsLeft.size() > 0) {
            logger.warn("Reached maximum retries count failing to load metadata for {} topics: {}", topicsLeft.size(),
                    topicsLeft.stream().map(TopicName::qualifiedName).collect(Collectors.joining(", ")));
        }
    }

    @Override
    public int getPriority() {
        return Hook.HIGHER_PRIORITY;
    }
}
