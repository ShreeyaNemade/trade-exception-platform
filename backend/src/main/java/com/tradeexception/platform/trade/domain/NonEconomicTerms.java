package com.tradeexception.platform.trade.domain;

import jakarta.validation.constraints.Size;

/**
 * Legal/operational terms that do not affect cash flows but are confirmed with the counterparty.
 * All fields are optional at ingestion; STP validation (later phase) decides which are mandatory.
 *
 * @param documentationType      master agreement type, e.g. {@code ISDA_2002}
 * @param governingLaw           e.g. {@code ENGLISH}, {@code NEW_YORK}
 * @param businessDayConvention  e.g. {@code MODIFIED_FOLLOWING}
 * @param calculationAgentPartyId party id acting as calculation agent
 */
public record NonEconomicTerms(
        @Size(max = 32) String documentationType,
        @Size(max = 32) String governingLaw,
        @Size(max = 32) String businessDayConvention,
        @Size(max = 20) String calculationAgentPartyId) {
}
