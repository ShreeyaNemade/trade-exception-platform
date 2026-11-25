package com.tradeexception.platform.enrichment;

import com.tradeexception.platform.common.couchdb.CouchDbConflictException;
import com.tradeexception.platform.common.couchdb.CouchDbConnectionException;
import com.tradeexception.platform.enrichment.domain.ReferenceData;
import com.tradeexception.platform.enrichment.domain.ReferenceDataCategory;
import com.tradeexception.platform.enrichment.repository.ReferenceDataRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ReferenceDataSeederTest {

    @Mock
    private ReferenceDataRepository repository;

    @Test
    void seedsMissingRecordsWithoutOverwritingExistingManualUpdates() {
        ReferenceData existingUsd = new ReferenceData();
        existingUsd.setCategory(ReferenceDataCategory.CURRENCY);
        existingUsd.setCode("USD");
        existingUsd.setDescription("Manually updated USD");

        when(repository.findByCategoryAndCode(any(), anyString())).thenReturn(Optional.empty());
        when(repository.findByCategoryAndCode(ReferenceDataCategory.CURRENCY, "USD"))
                .thenReturn(Optional.of(existingUsd));
        when(repository.save(any(ReferenceData.class))).thenAnswer(inv -> inv.getArgument(0));

        ReferenceDataSeeder seeder = new ReferenceDataSeeder(repository, new ReferenceDataProperties());
        int inserted = seeder.seedAll();

        assertThat(inserted).isEqualTo(ReferenceDataSeeder.defaultCatalog().size() - 1);
        verify(repository, never()).save(existingUsd);
    }

    @Test
    void handlesConcurrentConflictAndDisabledAutoSeed() {
        ReferenceDataProperties props = new ReferenceDataProperties();
        props.setAutoSeed(false);
        new ReferenceDataSeeder(repository, props).run(new DefaultApplicationArguments());
        verifyNoInteractions(repository);

        when(repository.findByCategoryAndCode(any(), anyString())).thenReturn(Optional.empty());
        when(repository.save(any())).thenThrow(new CouchDbConflictException("reference-data", "id", null, null));

        int inserted = new ReferenceDataSeeder(repository, new ReferenceDataProperties()).seedAll();
        assertThat(inserted).isZero();
    }

    @Test
    void respectsFailFastWhenCouchDbUnavailable() {
        when(repository.findByCategoryAndCode(any(), anyString()))
                .thenThrow(new CouchDbConnectionException("CouchDB down", null));

        ReferenceDataProperties props = new ReferenceDataProperties();
        props.setFailFast(false);
        new ReferenceDataSeeder(repository, props).run(new DefaultApplicationArguments());

        props.setFailFast(true);
        ReferenceDataSeeder strict = new ReferenceDataSeeder(repository, props);
        assertThatThrownBy(() -> strict.run(new DefaultApplicationArguments()))
                .isInstanceOf(CouchDbConnectionException.class);
    }
}
