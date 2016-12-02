package pl.allegro.tech.hermes.frontend.server

import com.codahale.metrics.MetricRegistry
import org.glassfish.hk2.api.ServiceLocator
import pl.allegro.tech.hermes.api.ContentType
import pl.allegro.tech.hermes.common.config.ConfigFactory
import pl.allegro.tech.hermes.common.kafka.KafkaTopic
import pl.allegro.tech.hermes.common.kafka.KafkaTopicName
import pl.allegro.tech.hermes.common.kafka.KafkaTopics
import pl.allegro.tech.hermes.common.metric.HermesMetrics
import pl.allegro.tech.hermes.domain.group.GroupRepository
import pl.allegro.tech.hermes.domain.topic.TopicRepository
import pl.allegro.tech.hermes.frontend.cache.topic.TopicsCache
import pl.allegro.tech.hermes.frontend.metric.CachedTopic
import pl.allegro.tech.hermes.frontend.producer.BrokerMessageProducer
import pl.allegro.tech.hermes.metrics.PathsCompiler
import pl.allegro.tech.hermes.test.helper.builder.TopicBuilder
import spock.lang.Shared
import spock.lang.Specification

class TopicMetadataLoadingStartupHookTest extends Specification {

    @Shared
    HermesMetrics hermesMetrics = new HermesMetrics(new MetricRegistry(), new PathsCompiler("localhost"))

    @Shared
    List<String> topics = ["g1.topicA", "g1.topicB", "g2.topicC"]

    @Shared
    GroupRepository groupRepository = Mock() {
        listGroupNames() >> ["g1", "g2"]
    }

    @Shared
    TopicRepository topicRepository = Mock() {
        listTopicNames("g1") >> ["topicA", "topicB"]
        listTopicNames("g2") >> ["topicC"]
    }

    @Shared
    Map<String, Optional<CachedTopic>> cachedTopics = new HashMap<>()

    @Shared
    TopicsCache topicsCache = Mock() {
        getTopic(_) >> { String topic -> cachedTopics.get(topic) }
    }

    @Shared
    ServiceLocator serviceLocator = Mock()

    def setupSpec() {
        for (String topic : topics) {
            cachedTopics.put(topic, getCachedTopic(topic))
        }
    }

    def "should load topic metadata"() {
        given:
        BrokerMessageProducer producer = Mock()
        def hook = new TopicMetadataLoadingStartupHook(producer, topicsCache, groupRepository, topicRepository, new ConfigFactory())

        when:
        hook.accept(serviceLocator)

        then:
        for (String topic : topics) {
            1 * producer.isTopicAvailable(cachedTopics.get(topic).get()) >> true
        }
    }

    def "should retry loading topic metadata"() {
        given:
        BrokerMessageProducer producer = Mock()
        def hook = new TopicMetadataLoadingStartupHook(producer, topicsCache, groupRepository, topicRepository, 2, 10L)

        when:
        hook.accept(serviceLocator)

        then:
        1 * producer.isTopicAvailable(cachedTopics.get("g1.topicA").get()) >> false
        1 * producer.isTopicAvailable(cachedTopics.get("g1.topicA").get()) >> true

        1 * producer.isTopicAvailable(cachedTopics.get("g1.topicB").get()) >> true

        2 * producer.isTopicAvailable(cachedTopics.get("g2.topicC").get()) >> false
        1 * producer.isTopicAvailable(cachedTopics.get("g2.topicC").get()) >> true
    }

    def "should leave retry loop when reached max retries and failed to load metadata"() {
        given:
        BrokerMessageProducer producer = Mock()
        def hook = new TopicMetadataLoadingStartupHook(producer, topicsCache, groupRepository, topicRepository, 2, 10L)

        when:
        hook.accept(serviceLocator)

        then:
        3 * producer.isTopicAvailable(cachedTopics.get("g1.topicA").get()) >> false
        1 * producer.isTopicAvailable(cachedTopics.get("g1.topicB").get()) >> true
        1 * producer.isTopicAvailable(cachedTopics.get("g2.topicC").get()) >> true
    }

    Optional<CachedTopic> getCachedTopic(String name) {
        def kafkaTopics = new KafkaTopics(new KafkaTopic(KafkaTopicName.valueOf(name), ContentType.JSON))
        def topic = new CachedTopic(TopicBuilder.topic(name).build(), hermesMetrics,
                kafkaTopics)
        return Optional.of(topic)
    }
}
