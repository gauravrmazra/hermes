package pl.allegro.tech.hermes.frontend.server;

import org.glassfish.hk2.api.ServiceLocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.allegro.tech.hermes.api.ContentType;
import pl.allegro.tech.hermes.api.Topic;
import pl.allegro.tech.hermes.common.config.ConfigFactory;
import pl.allegro.tech.hermes.common.hook.Hook;
import pl.allegro.tech.hermes.common.hook.ServiceAwareHook;
import pl.allegro.tech.hermes.domain.group.GroupRepository;
import pl.allegro.tech.hermes.domain.topic.TopicRepository;
import pl.allegro.tech.hermes.schema.CouldNotLoadSchemaException;
import pl.allegro.tech.hermes.schema.SchemaNotFoundException;
import pl.allegro.tech.hermes.schema.SchemaRepository;

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
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_RETRY_COUNT;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_THREAD_POOL_SIZE;

public class TopicSchemaLoadingStartupHook implements ServiceAwareHook {

    private static final Logger logger = LoggerFactory.getLogger(TopicSchemaLoadingStartupHook.class);

    private final GroupRepository groupRepository;

    private final TopicRepository topicRepository;

    private final SchemaRepository schemaRepository;

    private final int retryCount;
    private final int threadPoolSize;

    @Inject
    public TopicSchemaLoadingStartupHook(GroupRepository groupRepository,
                                         TopicRepository topicRepository,
                                         SchemaRepository schemaRepository,
                                         ConfigFactory config) {
        this(groupRepository, topicRepository, schemaRepository,
                config.getIntProperty(FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_RETRY_COUNT),
                config.getIntProperty(FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_THREAD_POOL_SIZE));
    }

    public TopicSchemaLoadingStartupHook(GroupRepository groupRepository,
                                         TopicRepository topicRepository,
                                         SchemaRepository schemaRepository,
                                         int retryCount,
                                         int threadPoolSize) {

        this.groupRepository = groupRepository;
        this.topicRepository = topicRepository;
        this.schemaRepository = schemaRepository;
        this.retryCount = retryCount;
        this.threadPoolSize = threadPoolSize;
    }

    @Override
    public void accept(ServiceLocator serviceLocator) {

        long start = System.currentTimeMillis();
        logger.info("Loading topic schemas");
        ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
        try {
            loadTopicSchemas(executorService);
        } finally {
            executorService.shutdown();
        }
        logger.info("Loading topic schemas done in {}ms", System.currentTimeMillis() - start);
    }

    private void loadTopicSchemas(ExecutorService executorService) {
        List<Topic> topics = getAvroTopics();

        List<LoadingResult> successes = new ArrayList<>();
        List<LoadingResult> notFound = new ArrayList<>();
        List<LoadingResult> failures = Collections.emptyList();

        int retriesLeft = retryCount + 1;
        while (topics.size() > 0 && retriesLeft > 0) {
            retriesLeft--;
            List<CompletableFuture<LoadingResult>> futures = submitSchemaLoadingTasks(executorService, topics);
            List<LoadingResult> allResults = whenAllComplete(futures).join();

            Map<LoadingResult.Type, List<LoadingResult>> groupedResults = getGroupedResults(allResults);

            Optional.ofNullable(groupedResults.get(LoadingResult.Type.NOT_FOUND)).ifPresent(notFound::addAll);
            Optional.ofNullable(groupedResults.get(LoadingResult.Type.SUCCESS)).ifPresent(successes::addAll);

            failures = Optional.ofNullable(groupedResults.get(LoadingResult.Type.FAILURE)).orElse(Collections.emptyList());
            topics = failures.stream().map(LoadingResult::getTopic).collect(toList());
        }
        logResultInfo(successes, failures, notFound);
    }

    private Map<LoadingResult.Type, List<LoadingResult>> getGroupedResults(List<LoadingResult> allResults) {
        return allResults.stream().collect(Collectors.groupingBy(LoadingResult::getType, Collectors.toList()));
    }

    private List<CompletableFuture<LoadingResult>> submitSchemaLoadingTasks(ExecutorService executorService, List<Topic> topics) {
        List<CompletableFuture<LoadingResult>> futures = new ArrayList<>();
        for (Topic topic : topics) {
            CompletableFuture<LoadingResult> futureResult = CompletableFuture.supplyAsync(() -> loadTopicSchema(topic), executorService);
            futures.add(futureResult);
        }
        return futures;
    }

    private LoadingResult loadTopicSchema(Topic topic) {
        try {
            schemaRepository.getLatestAvroSchema(topic);
            logger.info("Successfully loaded schema for topic {}", topic.getQualifiedName());
            return LoadingResult.success(topic);
        } catch (SchemaNotFoundException e) {
            logger.warn("Could not load schema for topic {}. {}", topic.getQualifiedName(), e.getMessage());
            return LoadingResult.notFound(topic);
        } catch (CouldNotLoadSchemaException e) {
            logger.error("An error occurred while loading schema for topic {}", topic.getQualifiedName(), e);
        }
        return LoadingResult.failure(topic);
    }

    private CompletableFuture<List<LoadingResult>> whenAllComplete(List<CompletableFuture<LoadingResult>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

    private List<Topic> getAvroTopics() {
        return groupRepository.listGroupNames().stream()
                .map(topicRepository::listTopics)
                .flatMap(List::stream)
                .filter(topic -> ContentType.AVRO == topic.getContentType())
                .collect(toList());
    }

    private void logResultInfo(List<LoadingResult> successes, List<LoadingResult> failures, List<LoadingResult> notFound) {
        if (!successes.isEmpty()) {
            logger.info("Successfully loaded schemas for {} topics", successes.size());
        }
        if (!notFound.isEmpty()) {
            logger.warn("Could not find schemas for {} topics: {}", notFound.size(),
                    notFound.stream().map(LoadingResult::getTopic).map(Topic::getQualifiedName)
                            .collect(Collectors.joining(", ")));
        }
        if (!failures.isEmpty()) {
            logger.warn("Failed to load schemas for {} topics, reached maximum retries count, failed topics: {}", failures.size(),
                    failures.stream().map(LoadingResult::getTopic).map(Topic::getQualifiedName)
                            .collect(Collectors.joining(", ")));
        }

    }

    @Override
    public int getPriority() {
        return Hook.HIGHER_PRIORITY;
    }

    private static final class LoadingResult {

        private enum Type {
            SUCCESS, FAILURE, NOT_FOUND
        }

        private final Type type;

        private final Topic topic;

        private LoadingResult(Type type, Topic topic) {
            this.type = type;
            this.topic = topic;
        }

        static LoadingResult success(Topic topic) {
            return new LoadingResult(Type.SUCCESS, topic);
        }

        static LoadingResult failure(Topic topic) {
            return new LoadingResult(Type.FAILURE, topic);
        }

        static LoadingResult notFound(Topic topic) {
            return new LoadingResult(Type.NOT_FOUND, topic);
        }

        public Type getType() {
            return type;
        }

        Topic getTopic() {
            return topic;
        }
    }
}
