package com.tradeexception.platform.common.exception;

import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.health.HealthController;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class GlobalExceptionHandlerTest {

    private final GlobalExceptionHandler handler = new GlobalExceptionHandler();

    private final MockMvc mockMvc = MockMvcBuilders
            .standaloneSetup(new HealthController("test-app"))
            .setControllerAdvice(handler)
            .build();

    @Test
    void unsupportedMethodReturns405InStandardEnvelope() throws Exception {
        mockMvc.perform(post("/api/v1/health"))
                .andExpect(status().isMethodNotAllowed())
                .andExpect(jsonPath("$.success").value(false))
                .andExpect(jsonPath("$.error.code").value("METHOD_NOT_ALLOWED"));
    }

    @Test
    void couchDbExceptionsMapToExpectedHttpStatuses() {
        ResponseEntity<?> conflict = handler.handlePlatform(
                new CouchDbConflictException("trades", "trade:TRD-1", "1-abc", null));
        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.CONFLICT);

        ResponseEntity<?> unavailable = handler.handlePlatform(
                new CouchDbConnectionException("CouchDB unreachable", null));
        assertThat(unavailable.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    }
}
