package pl.allegro.tech.hermes.frontend.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.TopicName;
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache;
import pl.allegro.tech.hermes.frontend.metric.CachedTopic;
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;

class TopicMetadataLoader implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataLoader.class);

    private final TopicsCache topicsCache;

    private final BrokerMessageProducer brokerMessageProducer;

    private final ScheduledExecutorService scheduler;

    private final RetryPolicy retryPolicy;

    TopicMetadataLoader(TopicsCache topicsCache,
                               BrokerMessageProducer brokerMessageProducer,
                               int retryCount,
                               long retryInterval,
                               int threadPoolSize) {
        this.topicsCache = topicsCache;

        this.brokerMessageProducer = brokerMessageProducer;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("topic-metadata-loader-%d").build();
        this.scheduler = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
        this.retryPolicy = new RetryPolicy()
                .withMaxRetries(retryCount)
                .withDelay(retryInterval, TimeUnit.MILLISECONDS)
                .retryIf(MetadataLoadingResult::isFailure);
    }

    CompletableFuture<MetadataLoadingResult> loadTopicMetadata(TopicName topic) {
        return Failsafe.with(retryPolicy).with(scheduler).future(() -> fetchTopicMetadata(topic));
    }

    private CompletableFuture<MetadataLoadingResult> fetchTopicMetadata(TopicName topic) {
        Optional<CachedTopic> cachedTopic = topicsCache.getTopic(topic.qualifiedName());
        if (cachedTopic.isPresent()) {
            if (brokerMessageProducer.isTopicAvailable(cachedTopic.get())) {
                logger.info("Topic {} metadata available", topic.qualifiedName());
                return completedFuture(MetadataLoadingResult.success(topic));
            } else {
                logger.warn("Topic {} metadata not available", topic.qualifiedName());
            }
        } else {
            logger.warn("Could not load topic {} metadata, topic not found in cache", topic.qualifiedName());
        }
        return completedFuture(MetadataLoadingResult.failure(topic));
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdown();
        scheduler.awaitTermination(1, TimeUnit.SECONDS);
    }
}
