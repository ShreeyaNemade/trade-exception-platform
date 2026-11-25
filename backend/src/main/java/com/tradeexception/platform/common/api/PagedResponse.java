package com.tradeexception.platform.common.api;

import java.util.List;

/** Standard paginated response envelope for list endpoints. */
public record PagedResponse<T>(
        List<T> items,
        int page,
        int size,
        long totalElements,
        int totalPages,
        boolean hasNext,
        boolean hasPrevious,
        String sortBy,
        String sortDir) {

    public static final int DEFAULT_MAX_PAGE_SIZE = 200;

    public static <T> PagedResponse<T> of(List<T> sortedItems, int page, int size, String sortBy, String sortDir) {
        return of(sortedItems, page, size, DEFAULT_MAX_PAGE_SIZE, sortBy, sortDir);
    }

    public static <T> PagedResponse<T> of(
            List<T> sortedItems,
            int page,
            int size,
            int maxSize,
            String sortBy,
            String sortDir) {
        int safePage = Math.max(0, page);
        int safeMaxSize = Math.max(1, maxSize);
        int safeSize = Math.min(Math.max(1, size), safeMaxSize);
        int total = sortedItems.size();
        int totalPages = total == 0 ? 0 : (int) Math.ceil((double) total / safeSize);
        long fromLong = (long) safePage * safeSize;
        int from = (int) Math.min(fromLong, total);
        int to = (int) Math.min(fromLong + safeSize, total);
        boolean hasPrevious = totalPages > 0 && safePage > 0;
        boolean hasNext = totalPages > 0 && safePage < (totalPages - 1);
        return new PagedResponse<>(
                List.copyOf(sortedItems.subList(from, to)),
                safePage,
                safeSize,
                total,
                totalPages,
                hasNext,
                hasPrevious,
                sortBy,
                sortDir);
    }
}
