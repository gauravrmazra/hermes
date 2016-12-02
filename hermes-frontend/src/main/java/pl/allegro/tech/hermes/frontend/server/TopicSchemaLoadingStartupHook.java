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
import java.util.List;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static pl.allegro.tech.hermes.common.config.Configs.FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_RETRY_COUNT;

public class TopicSchemaLoadingStartupHook implements ServiceAwareHook {

    private static final Logger logger = LoggerFactory.getLogger(TopicSchemaLoadingStartupHook.class);

    private final GroupRepository groupRepository;

    private final TopicRepository topicRepository;

    private final SchemaRepository schemaRepository;

    private final int retryCount;

    @Inject
    public TopicSchemaLoadingStartupHook(GroupRepository groupRepository,
                                         TopicRepository topicRepository,
                                         SchemaRepository schemaRepository,
                                         ConfigFactory config) {
        this(groupRepository, topicRepository, schemaRepository, config.getIntProperty(FRONTEND_STARTUP_TOPIC_SCHEMA_LOADING_RETRY_COUNT));
    }

    public TopicSchemaLoadingStartupHook(GroupRepository groupRepository,
                                         TopicRepository topicRepository,
                                         SchemaRepository schemaRepository,
                                         int retryCount) {

        this.groupRepository = groupRepository;
        this.topicRepository = topicRepository;
        this.schemaRepository = schemaRepository;
        this.retryCount = retryCount;
    }

    @Override
    public void accept(ServiceLocator serviceLocator) {
        logger.info("Loading topic schemas");
        List<Topic> topicsLeft = getAvroTopics();

        int retries = retryCount + 1;

        while (retries > 0) {
            retries--;
            topicsLeft = topicsLeft.stream().filter(this::topicSchemaNotLoaded).collect(toList());
        }
        logger.info("Loading topic schemas finished");
        if (topicsLeft.size() > 0) {
            logger.warn("Reached maximum retries count failing to load schema for {} topics: {}", topicsLeft.size(),
                    topicsLeft.stream().map(Topic::getQualifiedName).collect(Collectors.joining(", ")));
        }
    }

    private List<Topic> getAvroTopics() {
        return groupRepository.listGroupNames().stream()
                .map(topicRepository::listTopics)
                .flatMap(List::stream)
                .filter(topic -> topic.getContentType().equals(ContentType.AVRO))
                .collect(toList());
    }

    private boolean topicSchemaNotLoaded(Topic topic) {
        return !loadTopicSchema(topic);
    }

    private boolean loadTopicSchema(Topic topic) {
        try {
            schemaRepository.getLatestAvroSchema(topic);
            logger.info("Successfully loaded schema for topic {}", topic.getQualifiedName());
            return true;
        } catch (SchemaNotFoundException e) {
            logger.warn("Could not load schema for topic {}. {}", topic.getQualifiedName(), e.getMessage());
            return true;
        } catch (CouldNotLoadSchemaException e) {
            logger.error("Could not load schema for topic {}", topic.getQualifiedName(), e);
        }
        return false;
    }

    @Override
    public int getPriority() {
        return Hook.HIGHER_PRIORITY;
    }
}
