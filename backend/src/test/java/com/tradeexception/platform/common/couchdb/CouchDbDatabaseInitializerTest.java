package com.tradeexception.platform.common.couchdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CouchDbDatabaseInitializerTest {

    @Mock
    private CouchDbClient client;

    @Test
    void createsOnlyMissingDatabasesAndEnsuresIndexes() {
        CouchDbProperties props = new CouchDbProperties();
        when(client.databaseExists("trades")).thenReturn(true);
        when(client.databaseExists("confirmations")).thenReturn(false);
        when(client.databaseExists("exceptions")).thenReturn(true);
        when(client.databaseExists("work-items")).thenReturn(false);
        when(client.databaseExists("audit-events")).thenReturn(true);
        when(client.databaseExists("reference-data")).thenReturn(false);
        when(client.databaseExists("dead-letters")).thenReturn(true);
        when(client.createDatabaseIfAbsent(anyString())).thenReturn(true);

        CouchDbDatabaseInitializer initializer = new CouchDbDatabaseInitializer(client, props);
        initializer.run(new DefaultApplicationArguments());

        verify(client, never()).createDatabaseIfAbsent("trades");
        verify(client, never()).createDatabaseIfAbsent("exceptions");
        verify(client, never()).createDatabaseIfAbsent("audit-events");
        verify(client, times(1)).createDatabaseIfAbsent("confirmations");
        verify(client, times(1)).createDatabaseIfAbsent("work-items");
        verify(client, times(1)).createDatabaseIfAbsent("reference-data");
        verify(client, atLeastOnce()).ensureIndex(anyString(), anyString(), anyList());
    }

    @Test
    void skipsInitializationWhenDisabled() {
        CouchDbProperties props = new CouchDbProperties();
        props.setAutoInitialize(false);

        new CouchDbDatabaseInitializer(client, props).run(new DefaultApplicationArguments());
        verifyNoInteractions(client);
    }

    @Test
    void respectsFailFastFlagWhenCouchDbIsUnavailable() {
        CouchDbProperties props = new CouchDbProperties();
        props.setFailFastOnInit(false);
        when(client.databaseExists(anyString())).thenThrow(new CouchDbConnectionException("down", null));

        // Does not throw when failFastOnInit is false
        new CouchDbDatabaseInitializer(client, props).run(new DefaultApplicationArguments());

        props.setFailFastOnInit(true);
        CouchDbDatabaseInitializer strict = new CouchDbDatabaseInitializer(client, props);
        assertThatThrownBy(() -> strict.run(new DefaultApplicationArguments()))
                .isInstanceOf(CouchDbConnectionException.class);
    }
}
