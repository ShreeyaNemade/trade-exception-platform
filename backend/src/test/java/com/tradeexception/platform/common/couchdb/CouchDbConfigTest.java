package com.tradeexception.platform.common.couchdb;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.header;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.headerDoesNotExist;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class CouchDbConfigTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withBean(RestClient.Builder.class, RestClient::builder)
            .withUserConfiguration(CouchDbConfig.class);

    @Test
    void bindsEnvironmentPropertiesAndDefaults() {
        contextRunner
                .withPropertyValues(
                        "app.couchdb.url=http://couch.internal:5984",
                        "app.couchdb.auto-initialize=false",
                        "app.couchdb.databases.trades=custom-trades")
                .run(ctx -> {
                    assertThat(ctx).hasSingleBean(CouchDbProperties.class);
                    assertThat(ctx).hasBean("couchDbRestClient");
                    CouchDbProperties props = ctx.getBean(CouchDbProperties.class);
                    assertThat(props.getUrl()).isEqualTo("http://couch.internal:5984");
                    assertThat(props.isAutoInitialize()).isFalse();
                    assertThat(props.getDatabases().getTrades()).isEqualTo("custom-trades");
                    assertThat(props.getDatabases().allNames()).containsExactly(
                            "custom-trades", "confirmations", "exceptions", "work-items", "audit-events", "reference-data", "dead-letters");
                });
    }

    @Test
    void configuresBasicAuthHeaderOnlyWhenUsernameProvided() {
        CouchDbProperties withAuth = new CouchDbProperties();
        withAuth.setUrl("http://couchdb.local:5984");
        withAuth.setUsername("tester");
        withAuth.setPassword("pass");

        RestClient.Builder authBuilder = RestClient.builder();
        MockRestServiceServer authServer = MockRestServiceServer.bindTo(authBuilder).build();
        RestClient authClient = CouchDbConfig.applyDefaults(authBuilder, withAuth).build();

        authServer.expect(requestTo("http://couchdb.local:5984/_up"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(header(HttpHeaders.AUTHORIZATION, "Basic dGVzdGVyOnBhc3M="))
                .andRespond(withSuccess());
        authClient.get().uri("/_up").retrieve().toBodilessEntity();
        authServer.verify();

        CouchDbProperties noAuth = new CouchDbProperties();
        noAuth.setUrl("http://couchdb.local:5984");
        noAuth.setUsername("");

        RestClient.Builder noAuthBuilder = RestClient.builder();
        MockRestServiceServer noAuthServer = MockRestServiceServer.bindTo(noAuthBuilder).build();
        RestClient noAuthClient = CouchDbConfig.applyDefaults(noAuthBuilder, noAuth).build();

        noAuthServer.expect(requestTo("http://couchdb.local:5984/_up"))
                .andExpect(method(HttpMethod.GET))
                .andExpect(headerDoesNotExist(HttpHeaders.AUTHORIZATION))
                .andRespond(withSuccess());
        noAuthClient.get().uri("/_up").retrieve().toBodilessEntity();
        noAuthServer.verify();
    }
}
