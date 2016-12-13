package pl.allegro.tech.hermes.frontend.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.frontend.metric.CachedTopic;
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;

class TopicMetadataLoader implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TopicMetadataLoader.class);

    private final BrokerMessageProducer brokerMessageProducer;

    private final ScheduledExecutorService scheduler;

    private final RetryPolicy retryPolicy;

    TopicMetadataLoader(BrokerMessageProducer brokerMessageProducer,
                               int retryCount,
                               long retryInterval,
                               int threadPoolSize) {

        this.brokerMessageProducer = brokerMessageProducer;
        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("topic-metadata-loader-%d").build();
        this.scheduler = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
        this.retryPolicy = new RetryPolicy()
                .withMaxRetries(retryCount)
                .withDelay(retryInterval, TimeUnit.MILLISECONDS)
                .retryIf(MetadataLoadingResult::isFailure);
    }

    CompletableFuture<MetadataLoadingResult> loadTopicMetadata(CachedTopic topic) {
        return Failsafe.with(retryPolicy).with(scheduler).future(() -> completedFuture(fetchTopicMetadata(topic)));
    }

    private MetadataLoadingResult fetchTopicMetadata(CachedTopic topic) {
        if (brokerMessageProducer.isTopicAvailable(topic)) {
            logger.info("Topic {} metadata available", topic.getQualifiedName());
            return MetadataLoadingResult.success(topic.getTopicName());
        }
        logger.warn("Topic {} metadata not available", topic.getQualifiedName());
        return MetadataLoadingResult.failure(topic.getTopicName());
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdown();
        scheduler.awaitTermination(1, TimeUnit.SECONDS);
    }
}
