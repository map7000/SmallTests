package ru.mfilatov.SmallTests.data;

import java.io.Serializable;
import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Transaction implements Serializable {
    @QuerySqlField
    private Long transactionId;
    @QuerySqlField(index = true)
    private Long clientId;
    @QuerySqlField
    private Long value;

    private Transaction(Builder builder) {
        transactionId = builder.transactionId;
        clientId = builder.clientId;
        value = builder.value;
    }

    public static final class Builder {

        private Long transactionId;
        private Long clientId;
        private Long value;

        public Builder() {
        }

        public Builder transactionId(Long val) {
            transactionId = val;
            return this;
        }

        public Builder clientId(Long val) {
            clientId = val;
            return this;
        }

        public Builder value(Long val) {
            value = val;
            return this;
        }

        public Transaction build() {
            return new Transaction(this);
        }
    }

}
