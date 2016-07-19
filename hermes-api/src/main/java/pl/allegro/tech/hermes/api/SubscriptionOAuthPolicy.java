package pl.allegro.tech.hermes.api;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class SubscriptionOAuthPolicy {

    public enum GrantType {
        CLIENT_CREDENTIALS, RESOURCE_OWNER_USERNAME_PASSWORD
    }

    @NotNull
    private final GrantType grantType;

    @NotNull
    private final String providerName;

    private final String scope;

    private final String username;

    private final String password;

    @JsonCreator
    public SubscriptionOAuthPolicy(
            @JsonProperty("grantType") GrantType grantType,
            @JsonProperty("providerName") String providerName,
            @JsonProperty("scope") String scope,
            @JsonProperty("username") String username,
            @JsonProperty("password") String password) {

        this.grantType = grantType;
        this.providerName = providerName;
        this.scope = scope;
        this.username = username;
        this.password = password;
    }

    public GrantType getGrantType() {
        return grantType;
    }

    public String getProviderName() {
        return providerName;
    }

    public String getScope() {
        return scope;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubscriptionOAuthPolicy that = (SubscriptionOAuthPolicy) o;
        return grantType == that.grantType &&
                Objects.equals(providerName, that.providerName) &&
                Objects.equals(scope, that.scope) &&
                Objects.equals(username, that.username) &&
                Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grantType, providerName, scope, username, password);
    }

    public static Builder subscriptionOAuthPolicyWithPasswordGrant(String providerName) {
        return new Builder(providerName, GrantType.RESOURCE_OWNER_USERNAME_PASSWORD);
    }

    public static Builder subscriptionOAuthPolicyWithClientCredentialsGrant(String providerName) {
        return new Builder(providerName, GrantType.CLIENT_CREDENTIALS);
    }

    public static class Builder {

        private final String providerName;
        private final GrantType grantType;
        private String scope;
        private String username;
        private String password;

        public Builder(String providerName, GrantType grantType) {
            this.providerName = providerName;
            this.grantType = grantType;
        }

        public Builder withScope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder withUsername(String username) {
            this.username = username;
            return this;
        }

        public Builder withPassword(String password) {
            this.password = password;
            return this;
        }

        public SubscriptionOAuthPolicy build() {
            return new SubscriptionOAuthPolicy(grantType, providerName, scope, username, password);
        }
    }
}
