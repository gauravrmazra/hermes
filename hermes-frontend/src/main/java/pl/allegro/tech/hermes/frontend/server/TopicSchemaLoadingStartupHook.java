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
import pl.allegro.tech.hermes.frontend.server.SchemaLoadingResult.Type;
import pl.allegro.tech.hermes.schema.SchemaRepository;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_RETRY_COUNT;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_THREAD_POOL_SIZE;
import static pl.allegro.tech.hermes.frontend.server.SchemaLoadingResult.Type.FAILURE;
import static pl.allegro.tech.hermes.frontend.server.SchemaLoadingResult.Type.NOT_FOUND;
import static pl.allegro.tech.hermes.frontend.server.SchemaLoadingResult.Type.SUCCESS;

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
        List<Topic> topics = getAvroTopics();
        List<SchemaLoadingResult> allResults = Collections.emptyList();

        try (TopicSchemaLoader loader = new TopicSchemaLoader(schemaRepository, retryCount, threadPoolSize)) {
            List<CompletableFuture<SchemaLoadingResult>> allFutures = new ArrayList<>();
            for (Topic topic : topics) {
                allFutures.add(loader.loadTopicSchema(topic));
            }
            allResults = whenAllComplete(allFutures).join();
        } catch (Exception e) {
            logger.error("An error occurred while loading schema topics", e);
        }
        logResultInfo(allResults);
        logger.info("Done loading topic schemas in {}ms", System.currentTimeMillis() - start);
    }

    private List<Topic> getAvroTopics() {
        return groupRepository.listGroupNames().stream()
                .map(topicRepository::listTopics)
                .flatMap(List::stream)
                .filter(topic -> ContentType.AVRO == topic.getContentType())
                .collect(toList());
    }

    private CompletableFuture<List<SchemaLoadingResult>> whenAllComplete(List<CompletableFuture<SchemaLoadingResult>> futures) {
        return CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[futures.size()]))
                .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(toList()));
    }

    private void logResultInfo(List<SchemaLoadingResult> allResults) {
        Map<Type, List<SchemaLoadingResult>> groupedResults = getGroupedResults(allResults);

        Optional.ofNullable(groupedResults.get(SUCCESS))
                .ifPresent(results -> logger.info("Successfully loaded schemas for {} topics", results.size()));

        Optional.ofNullable(groupedResults.get(NOT_FOUND))
                .ifPresent(results -> logger.warn("Could not find schemas for {} topics: {}", results.size(), topicsOfResults(results)));

        Optional.ofNullable(groupedResults.get(FAILURE))
                .ifPresent(results -> logger.warn("Failed to load schemas for {} topics, " +
                                "reached maximum retries count, failed topics: {}", results.size(), topicsOfResults(results)));
    }

    private String topicsOfResults(List<SchemaLoadingResult> results) {
        return results.stream()
                .map(SchemaLoadingResult::getTopic)
                .map(Topic::getQualifiedName)
                .collect(joining(", "));
    }

    private Map<Type, List<SchemaLoadingResult>> getGroupedResults(List<SchemaLoadingResult> allResults) {
        return allResults.stream().collect(Collectors.groupingBy(SchemaLoadingResult::getType, Collectors.toList()));
    }

    @Override
    public int getPriority() {
        return Hook.HIGHER_PRIORITY;
    }
}
