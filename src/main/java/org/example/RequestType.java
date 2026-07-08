package org.example;

public enum RequestType {
    AUTH_REQUEST((short)0),
    REGISTER_REQUEST((short)1),

    ADD_EVENT((short)2),
    NEW_DAY((short)3),

    AGGREGATE_QTY((short)4),
    AGGREGATE_VOL((short)5),
    AGGREGATE_AVG((short)6),
    AGGREGATE_MAX((short)7),

    FILTER_EVENTS((short)8),

    SUB_SIMULTANEOUS((short)9),
    SUB_CONSECUTIVE((short)10),

    DISCONNECT((short)11);

    private final short value;

    RequestType(short value) {
        this.value = value;
    }

    public short getValue() {
        return value;
    }

    public static RequestType fromId(short id) {
        for (RequestType type: values()) {
            if (type.getValue() == id) {
                return type;
            }
        }
        return null;
    }
}