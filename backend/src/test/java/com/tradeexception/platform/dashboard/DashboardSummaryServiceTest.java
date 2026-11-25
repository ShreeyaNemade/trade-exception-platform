package com.tradeexception.platform.dashboard;

import com.tradeexception.platform.confirmation.domain.Confirmation;
import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.exception.domain.ExceptionPriority;
import com.tradeexception.platform.exception.domain.ExceptionRecord;
import com.tradeexception.platform.exception.domain.ExceptionSeverity;
import com.tradeexception.platform.exception.domain.ExceptionStatus;
import com.tradeexception.platform.exception.repository.ExceptionRepository;
import com.tradeexception.platform.trade.domain.Trade;
import com.tradeexception.platform.trade.domain.TradeStatus;
import com.tradeexception.platform.trade.repository.TradeRepository;
import com.tradeexception.platform.workitem.domain.WorkItem;
import com.tradeexception.platform.workitem.domain.WorkItemStatus;
import com.tradeexception.platform.workitem.repository.WorkItemRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DashboardSummaryServiceTest {

    @Mock private TradeRepository tradeRepository;
    @Mock private ConfirmationRepository confirmationRepository;
    @Mock private ExceptionRepository exceptionRepository;
    @Mock private WorkItemRepository workItemRepository;

    @Test
    void aggregatesMetricsWithoutDoubleCountingLifecycleStages() {
        Trade t1 = trade("TRD-1", TradeStatus.CONFIRMED, LocalDate.of(2026, 11, 23));
        Trade t2 = trade("TRD-2", TradeStatus.EXCEPTION, LocalDate.of(2026, 11, 24));
        Trade t3 = trade("TRD-3", TradeStatus.EXCEPTION, LocalDate.of(2026, 11, 24));
        Trade t4 = trade("TRD-4", TradeStatus.ACCEPTED, LocalDate.of(2026, 11, 24));

        Confirmation c1 = new Confirmation();
        c1.setConfirmationId("CNF-TRD-1");

        ExceptionRecord rejected = new ExceptionRecord();
        rejected.setExceptionId("EXC-TRD-2");
        rejected.setTradeId("TRD-2");
        rejected.setCategory(ExceptionCategory.INVALID_ECONOMIC_TERMS);
        rejected.setPriority(ExceptionPriority.CRITICAL);
        rejected.setSeverity(ExceptionSeverity.CRITICAL);
        rejected.setStatus(ExceptionStatus.OPEN);
        rejected.setReasonCode("INVALID_ECONOMIC_TERMS");
        rejected.setDescription("Trade rejected by STP validation");

        ExceptionRecord manual = new ExceptionRecord();
        manual.setExceptionId("EXC-TRD-3");
        manual.setTradeId("TRD-3");
        manual.setCategory(ExceptionCategory.INVALID_COUNTERPARTY);
        manual.setPriority(ExceptionPriority.MEDIUM);
        manual.setSeverity(ExceptionSeverity.MEDIUM);
        manual.setStatus(ExceptionStatus.OPEN);
        manual.setReasonCode("COUNTERPARTY_NOT_STP_ELIGIBLE");
        manual.setDescription("Manual review required: restricted counterparty");

        WorkItem wiOpen = new WorkItem();
        wiOpen.setWorkItemId("WI-1");
        wiOpen.setStatus(WorkItemStatus.OPEN);
        WorkItem wiResolved = new WorkItem();
        wiResolved.setWorkItemId("WI-2");
        wiResolved.setStatus(WorkItemStatus.RESOLVED);

        when(tradeRepository.findAll()).thenReturn(List.of(t1, t2, t3, t4));
        when(confirmationRepository.findAll()).thenReturn(List.of(c1));
        when(exceptionRepository.findAll()).thenReturn(List.of(rejected, manual));
        when(workItemRepository.findAll()).thenReturn(List.of(wiOpen, wiResolved));

        DashboardSummaryDto summary = new DashboardSummaryService(
                tradeRepository, confirmationRepository, exceptionRepository, workItemRepository).getSummary();

        assertThat(summary.totalTrades()).isEqualTo(4);
        assertThat(summary.acceptedTrades()).isEqualTo(1);
        assertThat(summary.stpEligibleTrades()).isEqualTo(1);
        assertThat(summary.stpRejectedTrades()).isEqualTo(1);
        assertThat(summary.manualReviewRequired()).isEqualTo(1);
        assertThat(summary.confirmationsGenerated()).isEqualTo(1);
        assertThat(summary.openExceptions()).isEqualTo(2);
        assertThat(summary.criticalExceptions()).isEqualTo(1);
        assertThat(summary.openWorkItems()).isEqualTo(1);
        assertThat(summary.resolvedWorkItems()).isEqualTo(1);
        assertThat(summary.stpSuccessRatePercent()).isEqualTo(25.0);
        assertThat(summary.tradeVolumeByDate()).hasSize(2);
        assertThat(summary.exceptionCategoryDistribution()).containsEntry("INVALID_ECONOMIC_TERMS", 1L);
    }

    private static Trade trade(String id, TradeStatus status, LocalDate date) {
        Trade t = new Trade();
        t.setTradeId(id);
        t.setStatus(status);
        t.setTradeDate(date);
        return t;
    }
}
