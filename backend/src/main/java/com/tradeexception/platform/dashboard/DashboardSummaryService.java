package com.tradeexception.platform.dashboard;

import com.tradeexception.platform.confirmation.repository.ConfirmationRepository;
import com.tradeexception.platform.dashboard.DashboardSummaryDto.DailyVolumeDto;
import com.tradeexception.platform.exception.api.ExceptionDto;
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
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Aggregates operational KPIs and chart distributions from persisted CouchDB repositories. */
@Service
public class DashboardSummaryService {

    private final TradeRepository tradeRepository;
    private final ConfirmationRepository confirmationRepository;
    private final ExceptionRepository exceptionRepository;
    private final WorkItemRepository workItemRepository;

    public DashboardSummaryService(
            TradeRepository tradeRepository,
            ConfirmationRepository confirmationRepository,
            ExceptionRepository exceptionRepository,
            WorkItemRepository workItemRepository) {
        this.tradeRepository = tradeRepository;
        this.confirmationRepository = confirmationRepository;
        this.exceptionRepository = exceptionRepository;
        this.workItemRepository = workItemRepository;
    }

    public DashboardSummaryDto getSummary() {
        List<Trade> trades = tradeRepository.findAll();
        long confirmationsGenerated = confirmationRepository.findAll().size();
        List<ExceptionRecord> exceptions = exceptionRepository.findAll();
        List<WorkItem> workItems = workItemRepository.findAll();

        long totalTrades = trades.size();
        long acceptedTrades = countTrades(trades, TradeStatus.ACCEPTED);
        long enrichedTrades = countTrades(trades, TradeStatus.ENRICHED);
        long stpEligibleTrades = trades.stream()
                .filter(t -> t.getStatus() == TradeStatus.VALIDATED || t.getStatus() == TradeStatus.CONFIRMED)
                .count();

        // Partition active (unresolved) exception trades into manual-review vs STP-rejected (never double-counted)
        List<ExceptionDto> activeExceptionDtos = exceptions.stream()
                .filter(e -> e.getStatus() == ExceptionStatus.OPEN || e.getStatus() == ExceptionStatus.IN_PROGRESS)
                .map(ExceptionDto::from)
                .toList();
        Set<String> manualReviewTradeIds = activeExceptionDtos.stream()
                .filter(ExceptionDto::manualReviewRequired)
                .map(ExceptionDto::tradeId)
                .collect(Collectors.toSet());
        Set<String> rejectedTradeIds = activeExceptionDtos.stream()
                .filter(e -> !manualReviewTradeIds.contains(e.tradeId()))
                .map(ExceptionDto::tradeId)
                .collect(Collectors.toSet());
        // Include any trade explicitly in REJECTED status not already counted
        trades.stream()
                .filter(t -> t.getStatus() == TradeStatus.REJECTED && !manualReviewTradeIds.contains(t.getTradeId()))
                .map(Trade::getTradeId)
                .forEach(rejectedTradeIds::add);

        long manualReviewRequired = manualReviewTradeIds.size();
        long stpRejectedTrades = rejectedTradeIds.size();

        long openExceptions = exceptions.stream()
                .filter(e -> e.getStatus() == ExceptionStatus.OPEN || e.getStatus() == ExceptionStatus.IN_PROGRESS)
                .count();
        long criticalExceptions = exceptions.stream()
                .filter(e -> e.getStatus() == ExceptionStatus.OPEN || e.getStatus() == ExceptionStatus.IN_PROGRESS)
                .filter(e -> e.getPriority() == ExceptionPriority.CRITICAL || e.getSeverity() == ExceptionSeverity.CRITICAL)
                .count();

        long openWorkItems = workItems.stream()
                .filter(w -> w.getStatus() == WorkItemStatus.OPEN
                        || w.getStatus() == WorkItemStatus.ASSIGNED
                        || w.getStatus() == WorkItemStatus.IN_PROGRESS
                        || w.getStatus() == WorkItemStatus.PENDING)
                .count();
        long resolvedWorkItems = workItems.stream()
                .filter(w -> w.getStatus() == WorkItemStatus.RESOLVED || w.getStatus() == WorkItemStatus.CLOSED)
                .count();

        double stpSuccessRate = totalTrades == 0
                ? 0.0
                : Math.round(((double) stpEligibleTrades / totalTrades) * 1000.0) / 10.0;

        Map<String, Long> tradeStatusDist = new LinkedHashMap<>();
        for (TradeStatus s : TradeStatus.values()) {
            long c = countTrades(trades, s);
            if (c > 0) {
                tradeStatusDist.put(s.name(), c);
            }
        }

        Map<String, Long> volumeMap = new TreeMap<>();
        for (Trade t : trades) {
            if (t.getTradeDate() != null) {
                volumeMap.merge(t.getTradeDate().toString(), 1L, Long::sum);
            }
        }
        List<DailyVolumeDto> tradeVolumeByDate = volumeMap.entrySet().stream()
                .map(e -> new DailyVolumeDto(e.getKey(), e.getValue()))
                .toList();

        Map<String, Long> exceptionCategoryDist = exceptions.stream()
                .collect(Collectors.groupingBy(
                        e -> e.getCategory() == null ? "UNCLASSIFIED" : e.getCategory().name(),
                        TreeMap::new,
                        Collectors.counting()));

        Map<String, Long> workItemStatusDist = workItems.stream()
                .collect(Collectors.groupingBy(
                        w -> w.getStatus() == null ? "OPEN" : w.getStatus().name(),
                        TreeMap::new,
                        Collectors.counting()));

        return new DashboardSummaryDto(
                totalTrades,
                acceptedTrades,
                enrichedTrades,
                stpEligibleTrades,
                stpRejectedTrades,
                manualReviewRequired,
                confirmationsGenerated,
                openExceptions,
                criticalExceptions,
                openWorkItems,
                resolvedWorkItems,
                stpSuccessRate,
                tradeStatusDist,
                tradeVolumeByDate,
                exceptionCategoryDist,
                workItemStatusDist);
    }

    private static long countTrades(List<Trade> trades, TradeStatus status) {
        return trades.stream().filter(t -> t.getStatus() == status).count();
    }
}
