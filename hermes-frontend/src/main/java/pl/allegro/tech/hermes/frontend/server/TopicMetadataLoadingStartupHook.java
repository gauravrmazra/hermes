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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_METADATA_LOADING_THREAD_POOL_SIZE;

public class TopicMetadataLoadingStartupHook implements ServiceAwareHook {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataLoadingStartupHook.class);

    private final BrokerMessageProducer brokerMessageProducer;

    private final TopicsCache topicsCache;

    private final GroupRepository groupRepository;

    private final TopicRepository topicRepository;

    private final int retryCount;

    private final long retryInterval;

    private final int threadPoolSize;

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
                config.getLongProperty(Configs.FRONTEND_STARTUP_TOPIC_METADATA_LOADING_RETRY_INTERVAL),
                config.getIntProperty(FRONTEND_STARTUP_TOPIC_METADATA_LOADING_THREAD_POOL_SIZE));
    }

    public TopicMetadataLoadingStartupHook(BrokerMessageProducer brokerMessageProducer,
                                           TopicsCache topicsCache,
                                           GroupRepository groupRepository,
                                           TopicRepository topicRepository,
                                           int retryCount,
                                           long retryInterval,
                                           int threadPoolSize) {
        this.brokerMessageProducer = brokerMessageProducer;
        this.topicsCache = topicsCache;
        this.groupRepository = groupRepository;
        this.topicRepository = topicRepository;
        this.retryCount = retryCount;
        this.retryInterval = retryInterval;
        this.threadPoolSize = threadPoolSize;
    }

    @Override
    public void accept(ServiceLocator serviceLocator) {
        long start = System.currentTimeMillis();
        logger.info("Loading topics metadata");
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        try {
            loadTopicsMetadata(executorService);
        } finally {
            executorService.shutdown();
        }
        logger.info("Loading topics metadata done in {}ms", System.currentTimeMillis() - start);
    }

    private void loadTopicsMetadata(ExecutorService executorService) {
        List<TopicName> topics = getTopics();

        List<LoadingResult> successes = new ArrayList<>();
        List<LoadingResult> failures = Collections.emptyList();

        int retriesLeft = retryCount + 1;
        while (topics.size() > 0 && retriesLeft > 0) {
            retriesLeft--;
            if (retriesLeft < retryCount) {
                waitBeforeNextRetry();
            }
            List<CompletableFuture<LoadingResult>> futures = submitMetadataLoadingTasks(executorService, topics);
            List<LoadingResult> allResults = whenAllComplete(futures).join();

            Map<LoadingResult.Result, List<LoadingResult>> groupedResults = getGroupedResults(allResults);

            Optional.ofNullable(groupedResults.get(LoadingResult.Result.SUCCESS)).ifPresent(successes::addAll);
            failures = Optional.ofNullable(groupedResults.get(LoadingResult.Result.FAILURE)).orElse(Collections.emptyList());
            topics = failures.stream().map(LoadingResult::getTopicName).collect(toList());
        }
        logResultInfo(successes, failures);
    }

    private List<TopicName> getTopics() {
        return groupRepository.listGroupNames().stream()
                .flatMap(group -> topicRepository.listTopicNames(group).stream().map(topic -> new TopicName(group, topic)))
                .collect(toList());
    }

    private void waitBeforeNextRetry() {
        try {
            Thread.sleep(retryInterval);
        } catch (InterruptedException e) {
            logger.warn("Waiting for next retry of broker topics availability interrupted.");
        }
    }

    private List<CompletableFuture<LoadingResult>> submitMetadataLoadingTasks(ExecutorService executorService, List<TopicName> topics) {
        List<CompletableFuture<LoadingResult>> futures = new ArrayList<>();
        for (TopicName topic : topics) {
            CompletableFuture<LoadingResult> futureResult = CompletableFuture.supplyAsync(() -> loadTopicMetadata(topic), executorService);
            futures.add(futureResult);
        }
        return futures;
    }

    private LoadingResult loadTopicMetadata(TopicName topic) {
        Optional<CachedTopic> cachedTopic = topicsCache.getTopic(topic.qualifiedName());
        if (cachedTopic.isPresent()) {
            if (brokerMessageProducer.isTopicAvailable(cachedTopic.get())) {
                logger.info("Topic {} metadata available", topic.qualifiedName());
                return LoadingResult.success(topic);
            } else {
                logger.warn("Topic {} metadata not available yet", topic.qualifiedName());
            }
        } else {
            logger.warn("Could not load topic {} metadata, topic not found in cache", topic.qualifiedName());
        }
        return LoadingResult.failure(topic);
    }

    private CompletableFuture<List<LoadingResult>> whenAllComplete(List<CompletableFuture<LoadingResult>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

    private Map<LoadingResult.Result, List<LoadingResult>> getGroupedResults(List<LoadingResult> allResults) {
        return allResults.stream().collect(Collectors.groupingBy(LoadingResult::getType, Collectors.toList()));
    }

    private void logResultInfo(List<LoadingResult> successes, List<LoadingResult> failures) {
        if (successes.size() > 0) {
            logger.info("Successfully loaded metadata for {} topics", successes.size());
        }
        if (failures.size() > 0) {
            logger.warn("Failed to load metadata for {} topics, reached maximum retries count, failed topics: {}", failures.size(),
                    failures.stream().map(LoadingResult::getTopicName).map(TopicName::qualifiedName)
                            .collect(Collectors.joining(", ")));
        }
    }

    @Override
    public int getPriority() {
        return Hook.HIGHER_PRIORITY;
    }

    private static final class LoadingResult {

        private enum Result { SUCCESS, FAILURE }

        private final Result type;

        private final TopicName topicName;

        LoadingResult(Result type, TopicName topicName) {
            this.type = type;
            this.topicName = topicName;
        }

        static LoadingResult success(TopicName topicName) {
            return new LoadingResult(Result.SUCCESS, topicName);
        }

        static LoadingResult failure(TopicName topicName) {
            return new LoadingResult(Result.FAILURE, topicName);
        }

        public Result getType() {
            return type;
        }

        public TopicName getTopicName() {
            return topicName;
        }
    }
}
