package com.tradeexception.platform.dashboard;

import java.util.List;
import java.util.Map;

/**
 * Aggregated operational metrics computed from persisted CouchDB documents.
 * <p>
 * Counting definitions (no double-counting across lifecycle stages):
 * <ul>
 *   <li>{@code totalTrades}: total distinct trades persisted in {@code trades}.</li>
 *   <li>{@code acceptedTrades}: trades currently in {@code ACCEPTED} status.</li>
 *   <li>{@code enrichedTrades}: trades currently in {@code ENRICHED} status.</li>
 *   <li>{@code stpEligibleTrades}: trades that passed STP validation ({@code VALIDATED} or {@code CONFIRMED}).</li>
 *   <li>{@code stpRejectedTrades}: distinct trades rejected by STP (excluding manual-review-only exceptions).</li>
 *   <li>{@code manualReviewRequired}: distinct trades flagged for manual operational review.</li>
 *   <li>{@code confirmationsGenerated}: total confirmation documents in {@code confirmations}.</li>
 *   <li>{@code openExceptions}: exceptions in {@code OPEN} or {@code IN_PROGRESS} status.</li>
 *   <li>{@code criticalExceptions}: open exceptions with {@code CRITICAL} priority or severity.</li>
 *   <li>{@code openWorkItems}: work items in {@code OPEN}, {@code ASSIGNED}, {@code IN_PROGRESS}, or {@code PENDING}.</li>
 *   <li>{@code resolvedWorkItems}: work items in {@code RESOLVED} or {@code CLOSED}.</li>
 * </ul>
 */
public record DashboardSummaryDto(
        long totalTrades,
        long acceptedTrades,
        long enrichedTrades,
        long stpEligibleTrades,
        long stpRejectedTrades,
        long manualReviewRequired,
        long confirmationsGenerated,
        long openExceptions,
        long criticalExceptions,
        long openWorkItems,
        long resolvedWorkItems,
        double stpSuccessRatePercent,
        Map<String, Long> tradeStatusDistribution,
        List<DailyVolumeDto> tradeVolumeByDate,
        Map<String, Long> exceptionCategoryDistribution,
        Map<String, Long> workItemStatusDistribution) {

    public record DailyVolumeDto(String date, long count) {
    }
}
