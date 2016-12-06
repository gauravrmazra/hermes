package pl.allegro.tech.hermes.frontend.server

import org.apache.avro.Schema
import org.glassfish.hk2.api.ServiceLocator
import pl.allegro.tech.hermes.api.ContentType
import pl.allegro.tech.hermes.api.Topic
import pl.allegro.tech.hermes.domain.group.GroupRepository
import pl.allegro.tech.hermes.domain.topic.TopicRepository
import pl.allegro.tech.hermes.schema.CompiledSchema
import pl.allegro.tech.hermes.schema.CompiledSchemaRepository
import pl.allegro.tech.hermes.schema.SchemaRepository
import pl.allegro.tech.hermes.schema.SchemaVersion
import pl.allegro.tech.hermes.schema.SchemaVersionsRepository
import pl.allegro.tech.hermes.test.helper.avro.AvroUser
import pl.allegro.tech.hermes.test.helper.builder.TopicBuilder
import spock.lang.Shared
import spock.lang.Specification

class TopicSchemaLoadingStartupHookTest extends Specification {

    @Shared SchemaVersion version = SchemaVersion.valueOf(1)

    @Shared Topic avroTopic1 = avroTopic("g1.topic1")

    @Shared Topic avroTopic2 = avroTopic("g1.topic2")

    @Shared Topic jsonTopic1 = jsonTopic("g1.topic3")

    @Shared Topic avroTopic3 = avroTopic("g2.topic1")

    @Shared CompiledSchema<Schema> schema = new AvroUser().getCompiledSchema()

    @Shared
    GroupRepository groupRepository = Mock() {
        listGroupNames() >> ["g1", "g2"]
    }

    @Shared
    ServiceLocator serviceLocator = Mock()

    def "should load topic schema for Avro topics"() {
        given:
        CompiledSchemaRepository<Schema> compiledSchemaRepository = Mock()
        TopicRepository topicRepository = Mock() {
            listTopics("g1") >> [avroTopic1, avroTopic2, jsonTopic1]
            listTopics("g2") >> [avroTopic3]
        }
        def schemaRepository = new SchemaRepository(schemaVersionsRepositoryForAvroTopics(), compiledSchemaRepository)
        def hook = new TopicSchemaLoadingStartupHook(groupRepository, topicRepository, schemaRepository, 2, 2)

        when:
        hook.accept(serviceLocator)

        then:
        1 * compiledSchemaRepository.getSchema(avroTopic1, version) >> schema
        0 * compiledSchemaRepository.getSchema(jsonTopic1, version) >> schema
        1 * compiledSchemaRepository.getSchema(avroTopic2, version) >> schema
        1 * compiledSchemaRepository.getSchema(avroTopic3, version) >> schema
    }

    def "should retry to load topic schema for Avro topics"() {
        given:
        CompiledSchemaRepository<Schema> compiledSchemaRepository = Mock()
        TopicRepository topicRepository = Mock() {
            listTopics("g1") >> [avroTopic1, avroTopic2, jsonTopic1]
            listTopics("g2") >> [avroTopic3]
        }
        def schemaRepository = new SchemaRepository(schemaVersionsRepositoryForAvroTopics(), compiledSchemaRepository)
        def hook = new TopicSchemaLoadingStartupHook(groupRepository, topicRepository, schemaRepository, 2, 2)

        when:
        hook.accept(serviceLocator)

        then:
        1 * compiledSchemaRepository.getSchema(avroTopic1, version) >> { throw new RuntimeException("an error") }
        1 * compiledSchemaRepository.getSchema(avroTopic1, version) >> schema

        0 * compiledSchemaRepository.getSchema(jsonTopic1, version) >> schema
        1 * compiledSchemaRepository.getSchema(avroTopic2, version) >> schema

        1 * compiledSchemaRepository.getSchema(avroTopic3, version) >> { throw new RuntimeException("an error") }
        1 * compiledSchemaRepository.getSchema(avroTopic3, version) >> { throw new RuntimeException("an error") }
        1 * compiledSchemaRepository.getSchema(avroTopic3, version) >> schema
    }

    def "should not retry loading schema for topics that have no schema"() {
        given:
        CompiledSchemaRepository<Schema> compiledSchemaRepository = Mock()
        TopicRepository topicRepository = Mock() {
            listTopics("g1") >> [avroTopic1]
            listTopics("g2") >> [avroTopic3]
        }
        SchemaVersionsRepository schemaVersionsRepositoryForTopicsWithoutSchema = Mock()
        SchemaVersionsRepository schemaVersionsRepository = [
                versions: { Topic topic, boolean online -> topic == avroTopic1 ? [version]
                        : schemaVersionsRepositoryForTopicsWithoutSchema.versions(topic, online) }
        ] as SchemaVersionsRepository
        def schemaRepository = new SchemaRepository(schemaVersionsRepository, compiledSchemaRepository)
        def hook = new TopicSchemaLoadingStartupHook(groupRepository, topicRepository, schemaRepository, 2, 2)

        when:
        hook.accept(serviceLocator)

        then:
        1 * compiledSchemaRepository.getSchema(avroTopic1, version) >> schema
        0 * compiledSchemaRepository.getSchema(avroTopic3, version) >> schema

        1 * schemaVersionsRepositoryForTopicsWithoutSchema.versions(avroTopic3, _) >> []
    }

    def "should not throw exception when no topics exist"() {
        TopicRepository topicRepository = Mock() {
            listTopics("g1") >> []
            listTopics("g2") >> []
        }
        given:
        CompiledSchemaRepository<Schema> compiledSchemaRepository = Mock()
        SchemaVersionsRepository schemaVersionsRepository = Mock()
        def schemaRepository = new SchemaRepository(schemaVersionsRepository, compiledSchemaRepository)
        def hook = new TopicSchemaLoadingStartupHook(groupRepository, topicRepository, schemaRepository, 2, 2)

        when:
        hook.accept(serviceLocator)

        then:
        noExceptionThrown()
    }

    private SchemaVersionsRepository schemaVersionsRepositoryForAvroTopics() {
        [
            versions: { Topic topic, boolean online ->
                topic.contentType == ContentType.AVRO ? [version] : []
            }
        ] as SchemaVersionsRepository
    }

    private Topic avroTopic(String name) {
        createTopic(name, ContentType.AVRO)
    }

    private Topic jsonTopic(String name) {
        createTopic(name, ContentType.JSON)
    }

    private Topic createTopic(String name, ContentType contentType) {
        TopicBuilder.topic(name).withContentType(contentType).build()
    }
}
