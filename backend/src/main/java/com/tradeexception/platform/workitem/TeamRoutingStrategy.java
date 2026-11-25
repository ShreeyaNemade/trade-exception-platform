package com.tradeexception.platform.workitem;

import com.tradeexception.platform.exception.domain.ExceptionCategory;
import com.tradeexception.platform.workitem.domain.OperationalTeam;
import org.springframework.stereotype.Component;

/** Deterministic mapping from classified {@link ExceptionCategory} to synthetic {@link OperationalTeam}. */
@Component
public class TeamRoutingStrategy {

    public OperationalTeam assignTeam(ExceptionCategory category) {
        if (category == null) {
            return OperationalTeam.TRADE_OPERATIONS;
        }
        return switch (category) {
            case INVALID_COUNTERPARTY, COUNTERPARTY_UNKNOWN -> OperationalTeam.COUNTERPARTY_OPERATIONS;
            case UNSUPPORTED_PRODUCT, INVALID_ECONOMIC_TERMS, ECONOMIC_MISMATCH -> OperationalTeam.PRODUCT_CONTROL;
            case CONFIRMATION_TEMPLATE_MISSING -> OperationalTeam.CONFIRMATION_OPERATIONS;
            default -> OperationalTeam.TRADE_OPERATIONS;
        };
    }
}
