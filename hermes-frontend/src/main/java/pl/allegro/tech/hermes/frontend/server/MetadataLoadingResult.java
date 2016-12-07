package pl.allegro.tech.hermes.frontend.server;

import pl.allegro.tech.hermes.api.TopicName;

import java.util.Objects;

final class MetadataLoadingResult {

    enum Type { SUCCESS, FAILURE }

    private final Type type;

    private final TopicName topicName;

    MetadataLoadingResult(Type type, TopicName topicName) {
        this.type = type;
        this.topicName = topicName;
    }

    static MetadataLoadingResult success(TopicName topicName) {
        return new MetadataLoadingResult(Type.SUCCESS, topicName);
    }

    static MetadataLoadingResult failure(TopicName topicName) {
        return new MetadataLoadingResult(Type.FAILURE, topicName);
    }

    Type getType() {
        return type;
    }

    TopicName getTopicName() {
        return topicName;
    }

    boolean isFailure() {
        return Type.FAILURE == type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MetadataLoadingResult that = (MetadataLoadingResult) o;
        return type == that.type && Objects.equals(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, topicName);
    }
}
