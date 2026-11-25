package com.tradeexception.platform.exception.api;

import com.tradeexception.platform.stp.domain.StpValidationResult;
import com.tradeexception.platform.trade.api.TradeDto;

/** Outcome returned after applying trade corrections and executing STP revalidation. */
public record RemediationResultDto(
        ExceptionDto exception,
        TradeDto trade,
        StpValidationResult validationResult) {
}
