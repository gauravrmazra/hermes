package pl.allegro.tech.hermes.frontend.server;

import pl.allegro.tech.hermes.api.Topic;

import java.util.Objects;

final class SchemaLoadingResult {

    enum Type {
        SUCCESS, FAILURE, NOT_FOUND
    }

    private final Type type;

    private final Topic topic;

    private SchemaLoadingResult(Type type, Topic topic) {
        this.type = type;
        this.topic = topic;
    }

    static SchemaLoadingResult success(Topic topic) {
        return new SchemaLoadingResult(Type.SUCCESS, topic);
    }

    static SchemaLoadingResult failure(Topic topic) {
        return new SchemaLoadingResult(Type.FAILURE, topic);
    }

    static SchemaLoadingResult notFound(Topic topic) {
        return new SchemaLoadingResult(Type.NOT_FOUND, topic);
    }

    Type getType() {
        return type;
    }

    Topic getTopic() {
        return topic;
    }

    boolean isSuccess() {
        return Type.SUCCESS == type;
    }

    boolean isFailure() {
        return Type.FAILURE == type;
    }

    boolean isNotFound() {
        return Type.NOT_FOUND == type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaLoadingResult that = (SchemaLoadingResult) o;
        return type == that.type && Objects.equals(topic, that.topic);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, topic);
    }
}
