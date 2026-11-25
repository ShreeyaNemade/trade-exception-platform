/**
 * Confirmation Management: generates structured synthetic confirmations for STP-validated trades,
 * persists them in CouchDB, updates trade status, and publishes {@code confirmation.generated}.
 */
package com.tradeexception.platform.confirmation;
