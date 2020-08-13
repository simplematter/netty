/*
 * Copyright 2014 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package io.netty.handler.codec.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Return Code of {@link MqttConnAckMessage}
 */
public enum MqttConnectReturnCode {
    CONNECTION_ACCEPTED((byte) 0x00),
    //MQTT 3 codes
    CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION((byte) 0X01),
    CONNECTION_REFUSED_IDENTIFIER_REJECTED((byte) 0x02),
    CONNECTION_REFUSED_SERVER_UNAVAILABLE((byte) 0x03),
    CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD((byte) 0x04),
    CONNECTION_REFUSED_NOT_AUTHORIZED((byte) 0x05),
    //MQTT 5 codes
    CONNECTION_REFUSED_UNSPECIFIED_ERROR((byte) 0x80),
    CONNECTION_REFUSED_MALFORMED_PACKET((byte) 0x80),
    CONNECTION_REFUSED_PROTOCOL_ERROR((byte) 0x80),
    CONNECTION_REFUSED_IMPLEMENTATION_SPECIFIC((byte) 0x80),
    CONNECTION_REFUSED_UNSUPPORTED_PROTOCOL_VERSION((byte) 0x80),
    CONNECTION_REFUSED_CLIENT_IDENTIFIER_NOT_VALID((byte) 0x80),
    CONNECTION_REFUSED_BAD_USERNAME_OR_PASSWORD((byte) 0x80),
    CONNECTION_REFUSED_NOT_AUTHORIZED_5((byte) 0x80),
    CONNECTION_REFUSED_SERVER_UNAVAILABLE_5((byte) 0x80),
    CONNECTION_REFUSED_SERVER_BUSY((byte) 0x80),
    CONNECTION_REFUSED_BANNED((byte) 0x80),
    CONNECTION_REFUSED_BAD_AUTHENTICATION_METHOD((byte) 0x80),
    CONNECTION_REFUSED_TOPIC_NAME_INVALID((byte) 0x80),
    CONNECTION_REFUSED_PACKET_TOO_LARGE((byte) 0x80),
    CONNECTION_REFUSED_QUOTA_EXCEEDED((byte) 0x80),
    CONNECTION_REFUSED_PAYLOAD_FORMAT_INVALID((byte) 0x80),
    CONNECTION_REFUSED_RETAIN_NOT_SUPPORTED((byte) 0x80),
    CONNECTION_REFUSED_QOS_NOT_SUPPORTED((byte) 0x80),
    CONNECTION_REFUSED_USE_ANOTHER_SERVER((byte) 0x80),
    CONNECTION_REFUSED_SERVER_MOVED((byte) 0x80),
    CONNECTION_REFUSED_CONNECTION_RATE_EXCEEDED((byte) 0x80);

    private static final Map<Byte, MqttConnectReturnCode> VALUE_TO_CODE_MAP;

    static {
        final Map<Byte, MqttConnectReturnCode> valueMap = new HashMap<Byte, MqttConnectReturnCode>();
        for (MqttConnectReturnCode code: values()) {
            valueMap.put(code.byteValue, code);
        }
        VALUE_TO_CODE_MAP = Collections.unmodifiableMap(valueMap);
    }

    private final byte byteValue;

    MqttConnectReturnCode(byte byteValue) {
        this.byteValue = byteValue;
    }

    public byte byteValue() {
        return byteValue;
    }

    public static MqttConnectReturnCode valueOf(byte b) {
        if (VALUE_TO_CODE_MAP.containsKey(b)) {
            return VALUE_TO_CODE_MAP.get(b);
        }
        throw new IllegalArgumentException("unknown connect return code: " + (b & 0xFF));
    }
}
