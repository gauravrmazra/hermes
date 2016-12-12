package pl.allegro.tech.hermes.frontend.server;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.schema.CouldNotLoadSchemaException;
import pl.allegro.tech.hermes.schema.SchemaNotFoundException;
import pl.allegro.tech.hermes.schema.SchemaRepository;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;

class TopicSchemaLoader implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(TopicSchemaLoader.class);

    private final SchemaRepository schemaRepository;

    private final ScheduledExecutorService scheduler;

    private final RetryPolicy retryPolicy;

    TopicSchemaLoader(SchemaRepository schemaRepository, int retryCount, int threadPoolSize) {
        this.schemaRepository = schemaRepository;

        ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat("topic-schema-loader-%d").build();
        this.scheduler = Executors.newScheduledThreadPool(threadPoolSize, threadFactory);
        this.retryPolicy = new RetryPolicy()
                .withMaxRetries(retryCount)
                .retryIf(SchemaLoadingResult::isFailure);
    }

    CompletableFuture<SchemaLoadingResult> loadTopicSchema(Topic topic) {
        return Failsafe.with(retryPolicy).with(scheduler).future(() -> completedFuture(loadLatestSchema(topic)));
    }

    private SchemaLoadingResult loadLatestSchema(Topic topic) {
        try {
            schemaRepository.getLatestAvroSchema(topic);
            logger.info("Successfully loaded schema for topic {}", topic.getQualifiedName());
            return SchemaLoadingResult.success(topic);
        } catch (SchemaNotFoundException e) {
            logger.warn("Could not load schema for topic {}. {}", topic.getQualifiedName(), e.getMessage());
            return SchemaLoadingResult.notFound(topic);
        } catch (CouldNotLoadSchemaException e) {
            logger.error("An error occurred while loading schema for topic {}", topic.getQualifiedName());
        }
        return SchemaLoadingResult.failure(topic);
    }

    @Override
    public void close() throws Exception {
        scheduler.shutdown();
        scheduler.awaitTermination(1, TimeUnit.SECONDS);
    }
}
