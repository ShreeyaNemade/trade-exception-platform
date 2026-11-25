/**
 * Trade Ingestion: synthetic trade event generation, Kafka consumption from {@code trade.raw},
 * envelope and domain validation, idempotent persistence, and publishing to {@code trade.accepted}.
 */
package com.tradeexception.platform.ingestion;
