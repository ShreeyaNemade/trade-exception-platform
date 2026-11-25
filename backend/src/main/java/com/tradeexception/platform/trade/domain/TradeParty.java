package com.tradeexception.platform.trade.domain;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/**
 * A party to the trade.
 *
 * @param partyId   synthetic LEI-style identifier (max 20 characters)
 * @param partyName display name of the synthetic legal entity
 * @param role      buyer or seller
 */
public record TradeParty(
        @NotBlank @Size(max = 20) String partyId,
        @NotBlank @Size(max = 128) String partyName,
        @NotNull PartyRole role) {
}
