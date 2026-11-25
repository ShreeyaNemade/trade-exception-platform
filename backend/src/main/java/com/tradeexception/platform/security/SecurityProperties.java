package com.tradeexception.platform.security;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Environment-driven authentication and token properties ({@code app.security.*}).
 * Credentials are never hardcoded; users with blank passwords are not registered.
 */
@ConfigurationProperties(prefix = "app.security")
public class SecurityProperties {

    private String tokenSecret = "";
    private long tokenTtlSeconds = 3600;
    private UserSpec admin = new UserSpec("admin", "");
    private UserSpec operations = new UserSpec("operations", "");
    private UserSpec viewer = new UserSpec("viewer", "");

    public String getTokenSecret() { return tokenSecret; }
    public void setTokenSecret(String tokenSecret) { this.tokenSecret = tokenSecret; }
    public long getTokenTtlSeconds() { return tokenTtlSeconds; }
    public void setTokenTtlSeconds(long tokenTtlSeconds) { this.tokenTtlSeconds = tokenTtlSeconds; }
    public UserSpec getAdmin() { return admin; }
    public void setAdmin(UserSpec admin) { this.admin = admin; }
    public UserSpec getOperations() { return operations; }
    public void setOperations(UserSpec operations) { this.operations = operations; }
    public UserSpec getViewer() { return viewer; }
    public void setViewer(UserSpec viewer) { this.viewer = viewer; }

    public static class UserSpec {
        private String username;
        private String password;

        public UserSpec() {}
        public UserSpec(String username, String password) {
            this.username = username;
            this.password = password;
        }

        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
    }
}
