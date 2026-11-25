package com.tradeexception.platform.ingestion;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.tradeexception.platform.common.kafka.EventEnvelope;
import com.tradeexception.platform.common.kafka.KafkaProperties;
import com.tradeexception.platform.trade.domain.ProductType;
import com.tradeexception.platform.trade.domain.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class SyntheticTradeProducerTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    private SyntheticTradeProducer producer;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        producer = new SyntheticTradeProducer(kafkaTemplate, mapper, new KafkaProperties());
    }

    @Test
    void generatesUniqueIdentifiersForValidScenarios() {
        EventEnvelope<Trade> spot1 = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        EventEnvelope<Trade> spot2 = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_SPOT);
        EventEnvelope<Trade> fwd = producer.buildEnvelopeForScenario(SyntheticTradeScenario.VALID_FX_FORWARD);

        assertThat(spot1.eventId()).isNotEqualTo(spot2.eventId());
        assertThat(spot1.payload().getTradeId()).isNotEqualTo(spot2.payload().getTradeId());
        assertThat(spot1.correlationId()).isEqualTo(spot1.payload().getHeader().correlationId());
        assertThat(spot1.payload().getProductType()).isEqualTo(ProductType.FX_SPOT);
        assertThat(fwd.payload().getProductType()).isEqualTo(ProductType.FX_FORWARD);
        assertThat(fwd.payload().getMaturityDate()).isNotNull();
    }

    @Test
    void duplicateScenarioPublishesExactSameMessageTwice() {
        SyntheticTradeProducer.PublishResult result =
                producer.publishScenario(SyntheticTradeScenario.DUPLICATE_MESSAGE);

        assertThat(result.messagesPublished()).isEqualTo(2);
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2)).send(eq("trade.raw"), eq(result.tradeId()), payloadCaptor.capture());
        assertThat(payloadCaptor.getAllValues().get(0)).isEqualTo(payloadCaptor.getAllValues().get(1));
    }

    @Test
    void malformedScenarioPublishesBrokenJsonToTradeRaw() {
        SyntheticTradeProducer.PublishResult result =
                producer.publishScenario(SyntheticTradeScenario.MALFORMED_PAYLOAD);

        assertThat(result.messagesPublished()).isEqualTo(1);
        assertThat(result.tradeId()).isNull();
        verify(kafkaTemplate, times(1)).send(eq("trade.raw"), eq(result.eventId()), anyString());
    }
}
