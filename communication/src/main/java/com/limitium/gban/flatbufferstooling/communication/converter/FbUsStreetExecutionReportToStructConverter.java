package com.limitium.gban.flatbufferstooling.communication.converter;

import com.limitium.gban.communication.messages.domain.executionreports.flatbuffers.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class FbUsStreetExecutionReportToStructConverter
        extends FbModelToStructConverter<FbUsStreetExecutionReport> {

    @Override
    protected Class<FbUsStreetExecutionReport> getClazz() {
        return FbUsStreetExecutionReport.class;
    }

    public SchemaBuilder fillSchema(SchemaBuilder builder) {
        return builder.field("EXECUTION_REPORT__ID", Schema.INT64_SCHEMA)
                .field("EXECUTION_REPORT__EXEC_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EXECUTION_REPORT__LAST_QTY", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("EXECUTION_REPORT__TRANSACT_TIME", Schema.OPTIONAL_INT64_SCHEMA)
                .field("EXECUTION_REPORT__CAPACITY", Schema.OPTIONAL_STRING_SCHEMA)
                .field("EXECUTION_REPORT__LAST_PRICE", Schema.OPTIONAL_FLOAT64_SCHEMA)
                .field("FRONT_OFFICE__EXECUTION_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__INSTANCE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__ORDER_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__NAME", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__EXECUTION_REF_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__COUNTERPARTY_EXECUTION_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("FRONT_OFFICE__COUNTERPARTY_ORDER_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORDER__ORDER_TYPE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORDER__SIDE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORDER__INSTRUMENT_ID", Schema.OPTIONAL_INT64_SCHEMA)
                .field("ORDER__SECURITY_ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("ORDER__TIME_IN_FORCE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("TRADING_ACCOUNT__ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("TRADING_ACCOUNT__PARTY_SOURCE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("TRADING_ACCOUNT__PARTY_ID_SOURCE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("COUNTER_PARTY__ID", Schema.OPTIONAL_STRING_SCHEMA)
                .field("COUNTER_PARTY__PARTY_ROLE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("COUNTER_PARTY__PARTY_SOURCE", Schema.OPTIONAL_STRING_SCHEMA)
                .field("COUNTER_PARTY__PARTY_ID_SOURCE", Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Override
    public void fillStruct(Struct struct, FbUsStreetExecutionReport obj) {
        putValueToStruct(struct, "EXECUTION_REPORT__ID", obj.executionReport().id());
        putEnumValueToStruct(
                struct,
                "EXECUTION_REPORT__EXEC_TYPE",
                FbExecType.names,
                obj.executionReport().execType());
        putValueToStruct(struct, "EXECUTION_REPORT__LAST_QTY", obj.executionReport().lastQty());
        putValueToStruct(
                struct, "EXECUTION_REPORT__TRANSACT_TIME", obj.executionReport().transactTime());
        putEnumValueToStruct(
                struct,
                "EXECUTION_REPORT__CAPACITY",
                FbCapacity.names,
                obj.executionReport().capacity());
        putValueToStruct(struct, "EXECUTION_REPORT__LAST_PRICE", obj.executionReport().lastPrice());

        putValueToStruct(struct, "FRONT_OFFICE__EXECUTION_ID", obj.frontOffice().executionId());
        putValueToStruct(struct, "FRONT_OFFICE__INSTANCE", obj.frontOffice().instance());
        putValueToStruct(struct, "FRONT_OFFICE__ORDER_ID", obj.frontOffice().orderId());
        putValueToStruct(struct, "FRONT_OFFICE__NAME", obj.frontOffice().name());
        putValueToStruct(
                struct, "FRONT_OFFICE__EXECUTION_REF_ID", obj.frontOffice().executionRefId());
        putValueToStruct(
                struct,
                "FRONT_OFFICE__COUNTERPARTY_EXECUTION_ID",
                obj.frontOffice().counterpartyExecutionId());
        putValueToStruct(
                struct,
                "FRONT_OFFICE__COUNTERPARTY_ORDER_ID",
                obj.frontOffice().counterpartyOrderId());

        putEnumValueToStruct(
                struct, "ORDER__ORDER_TYPE", FbOrderType.names, obj.order().orderType());
        putEnumValueToStruct(struct, "ORDER__SIDE", FbSide.names, obj.order().side());
        putValueToStruct(struct, "ORDER__INSTRUMENT_ID", obj.order().instrumentId());
        putValueToStruct(struct, "ORDER__SECURITY_ID", obj.order().securityId());
        putEnumValueToStruct(
                struct, "ORDER__TIME_IN_FORCE", FbTimeInForce.names, obj.order().timeInForce());

        putValueToStruct(struct, "TRADING_ACCOUNT__ID", obj.tradingAccount().id());
        putEnumValueToStruct(
                struct, "TRADING_ACCOUNT__PARTY_SOURCE", FbPartySource.names, obj.tradingAccount().partySource());
        putEnumValueToStruct(
                struct, "TRADING_ACCOUNT__PARTY_ID_SOURCE", FbPartyIdSource.names, obj.tradingAccount().partyIdSource());

        putValueToStruct(struct, "COUNTER_PARTY__ID", obj.counterParty().id());
        putEnumValueToStruct(
                struct, "COUNTER_PARTY__PARTY_ROLE", FbPartyRole.names, obj.counterParty().partyRole());
        putEnumValueToStruct(
                struct,
                "COUNTER_PARTY__PARTY_SOURCE",
                FbPartySource.names,
                obj.counterParty().partySource());
        putEnumValueToStruct(
                struct,
                "COUNTER_PARTY__PARTY_ID_SOURCE",
                FbPartyIdSource.names,
                obj.counterParty().partyIdSource());
    }
}
