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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.EmptyByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.EmptyArrays;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static io.netty.handler.codec.mqtt.MqttCodecUtil.*;

/**
 * Encodes Mqtt messages into bytes following the protocol specification v3.1
 * as described here <a href="http://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html">MQTTV3.1</a>
 */
@ChannelHandler.Sharable
public final class MqttEncoder extends MessageToMessageEncoder<MqttMessage> {

    public static final MqttEncoder INSTANCE = new MqttEncoder(null);

    private AtomicReference<MqttVersion> mqttVersionRef;

    private MqttVersion mqttVersion() {
        if (mqttVersionRef == null) {
            return MqttVersion.MQTT_3_1_1;
        } else {
            return mqttVersionRef.get();
        }
    }

    /**
     * @param mqttVersionRef - version reference to be shared between encoder and decoder. If null - assumes MQTT 3
     */
    public MqttEncoder(AtomicReference<MqttVersion> mqttVersionRef) {
        this.mqttVersionRef = mqttVersionRef;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, MqttMessage msg, List<Object> out) throws Exception {
        out.add(doEncode(ctx.alloc(), msg));
    }

    /**
     * This is the main encoding method.
     * It's only visible for testing.
     *
     * @param byteBufAllocator Allocates ByteBuf
     * @param message          MQTT message to encode
     * @return ByteBuf with encoded bytes
     */
    ByteBuf doEncode(ByteBufAllocator byteBufAllocator, MqttMessage message) {

        switch (message.fixedHeader().messageType()) {
            case CONNECT:
                return encodeConnectMessage(byteBufAllocator, (MqttConnectMessage) message);

            case CONNACK:
                return encodeConnAckMessage(byteBufAllocator, (MqttConnAckMessage) message);

            case PUBLISH:
                return encodePublishMessage(byteBufAllocator, (MqttPublishMessage) message);

            case SUBSCRIBE:
                return encodeSubscribeMessage(byteBufAllocator, (MqttSubscribeMessage) message);

            case UNSUBSCRIBE:
                return encodeUnsubscribeMessage(byteBufAllocator, (MqttUnsubscribeMessage) message);

            case SUBACK:
                return encodeSubAckMessage(byteBufAllocator, (MqttSubAckMessage) message);

            case UNSUBACK:
                if (message instanceof MqttUnsubAckMessage) {
                    return encodeUnsubAckMessage(byteBufAllocator, (MqttUnsubAckMessage) message);
                } else {
                    return encodeMessageWithOnlySingleByteFixedHeaderAndMessageId(byteBufAllocator, message);
                }

            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
                return encodePubReplyMessage(byteBufAllocator, message);

            case DISCONNECT:
            case AUTH:
                return encodeReasonCodePlusPropertiesMessage(byteBufAllocator, message);

            case PINGREQ:
            case PINGRESP:
                return encodeMessageWithOnlySingleByteFixedHeader(byteBufAllocator, message);

            default:
                throw new IllegalArgumentException(
                        "Unknown message type: " + message.fixedHeader().messageType().value());
        }
    }

    private ByteBuf encodeConnectMessage(
            ByteBufAllocator byteBufAllocator,
            MqttConnectMessage message) {
        int payloadBufferSize = 0;

        if (mqttVersionRef != null) {
            mqttVersionRef.set(MqttVersion.fromProtocolNameAndLevel(message.variableHeader().name(),
                    (byte) message.variableHeader().version()));
        }

        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        MqttConnectVariableHeader variableHeader = message.variableHeader();
        MqttConnectPayload payload = message.payload();
        MqttVersion mqttVersion = MqttVersion.fromProtocolNameAndLevel(variableHeader.name(),
                (byte) variableHeader.version());

        // as MQTT 3.1 & 3.1.1 spec, If the User Name Flag is set to 0, the Password Flag MUST be set to 0
        if (!variableHeader.hasUserName() && variableHeader.hasPassword()) {
            throw new EncoderException("Without a username, the password MUST be not set");
        }

        // Client id
        String clientIdentifier = payload.clientIdentifier();
        if (!isValidClientId(mqttVersion, clientIdentifier)) {
            throw new MqttIdentifierRejectedException("invalid clientIdentifier: " + clientIdentifier);
        }
        byte[] clientIdentifierBytes = encodeStringUtf8(clientIdentifier);
        payloadBufferSize += 2 + clientIdentifierBytes.length;

        // Will topic and message
        String willTopic = payload.willTopic();
        byte[] willTopicBytes = willTopic != null ? encodeStringUtf8(willTopic) : EmptyArrays.EMPTY_BYTES;
        byte[] willMessage = payload.willMessageInBytes();
        byte[] willMessageBytes = willMessage != null ? willMessage : EmptyArrays.EMPTY_BYTES;
        if (variableHeader.isWillFlag()) {
            payloadBufferSize += 2 + willTopicBytes.length;
            payloadBufferSize += 2 + willMessageBytes.length;
        }

        String userName = payload.userName();
        byte[] userNameBytes = userName != null ? encodeStringUtf8(userName) : EmptyArrays.EMPTY_BYTES;
        if (variableHeader.hasUserName()) {
            payloadBufferSize += 2 + userNameBytes.length;
        }

        byte[] password = payload.passwordInBytes();
        byte[] passwordBytes = password != null ? password : EmptyArrays.EMPTY_BYTES;
        if (variableHeader.hasPassword()) {
            payloadBufferSize += 2 + passwordBytes.length;
        }

        // Fixed and variable header
        byte[] protocolNameBytes = mqttVersion.protocolNameBytes();
        int variableHeaderBufferSize = 2 + protocolNameBytes.length + 4;
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());
        ByteBuf willPropertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, payload.willProperties());
        payloadBufferSize += willPropertiesBuf.readableBytes();
        int variablePartSize = variableHeaderBufferSize + payloadBufferSize + propertiesBuf.readableBytes();
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);
        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        writeVariableLengthInt(buf, variablePartSize);

        buf.writeShort(protocolNameBytes.length);
        buf.writeBytes(protocolNameBytes);

        buf.writeByte(variableHeader.version());
        buf.writeByte(getConnVariableHeaderFlag(variableHeader));
        buf.writeShort(variableHeader.keepAliveTimeSeconds());
        buf.writeBytes(propertiesBuf);

        // Payload
        buf.writeShort(clientIdentifierBytes.length);
        buf.writeBytes(clientIdentifierBytes, 0, clientIdentifierBytes.length);
        if (variableHeader.isWillFlag()) {
            buf.writeBytes(willPropertiesBuf);
            buf.writeShort(willTopicBytes.length);
            buf.writeBytes(willTopicBytes, 0, willTopicBytes.length);
            buf.writeShort(willMessageBytes.length);
            buf.writeBytes(willMessageBytes, 0, willMessageBytes.length);
        }
        if (variableHeader.hasUserName()) {
            buf.writeShort(userNameBytes.length);
            buf.writeBytes(userNameBytes, 0, userNameBytes.length);
        }
        if (variableHeader.hasPassword()) {
            buf.writeShort(passwordBytes.length);
            buf.writeBytes(passwordBytes, 0, passwordBytes.length);
        }
        return buf;
    }

    private static int getConnVariableHeaderFlag(MqttConnectVariableHeader variableHeader) {
        int flagByte = 0;
        if (variableHeader.hasUserName()) {
            flagByte |= 0x80;
        }
        if (variableHeader.hasPassword()) {
            flagByte |= 0x40;
        }
        if (variableHeader.isWillRetain()) {
            flagByte |= 0x20;
        }
        flagByte |= (variableHeader.willQos() & 0x03) << 3;
        if (variableHeader.isWillFlag()) {
            flagByte |= 0x04;
        }
        if (variableHeader.isCleanSession()) {
            flagByte |= 0x02;
        }
        return flagByte;
    }

    private ByteBuf encodeConnAckMessage(
            ByteBufAllocator byteBufAllocator,
            MqttConnAckMessage message) {
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());

        ByteBuf buf = byteBufAllocator.buffer(4 + propertiesBuf.readableBytes());
        buf.writeByte(getFixedHeaderByte1(message.fixedHeader()));
        writeVariableLengthInt(buf, 2 + propertiesBuf.readableBytes());
        buf.writeByte(message.variableHeader().isSessionPresent() ? 0x01 : 0x00);
        buf.writeByte(message.variableHeader().connectReturnCode().byteValue());
        buf.writeBytes(propertiesBuf);

        return buf;
    }

    private ByteBuf encodeSubscribeMessage(
            ByteBufAllocator byteBufAllocator,
            MqttSubscribeMessage message) {
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());

        final int variableHeaderBufferSize = 2 + propertiesBuf.readableBytes();
        int payloadBufferSize = 0;

        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        MqttMessageIdVariableHeader variableHeader = message.variableHeader();
        MqttSubscribePayload payload = message.payload();

        for (MqttTopicSubscription topic : payload.topicSubscriptions()) {
            String topicName = topic.topicName();
            byte[] topicNameBytes = encodeStringUtf8(topicName);
            payloadBufferSize += 2 + topicNameBytes.length;
            payloadBufferSize += 1;
        }

        int variablePartSize = variableHeaderBufferSize + payloadBufferSize;
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        writeVariableLengthInt(buf, variablePartSize);

        // Variable Header
        int messageId = variableHeader.messageId();
        buf.writeShort(messageId);
        buf.writeBytes(propertiesBuf);

        // Payload
        for (MqttTopicSubscription topic : payload.topicSubscriptions()) {
            writeUTF8String(buf, topic.topicName());
            final MqttSubscriptionOption option = topic.option();

            int optionEncoded = 0;
            optionEncoded |= option.retainHandling().value() << 4;
            if (option.isRetainAsPublished()) {
                optionEncoded |= 0x08;
            }
            if (option.isNoLocal()) {
                optionEncoded |= 0x04;
            }
            optionEncoded |= option.qos().value();

            buf.writeByte(optionEncoded);
        }

        return buf;
    }

    private ByteBuf encodeUnsubscribeMessage(
            ByteBufAllocator byteBufAllocator,
            MqttUnsubscribeMessage message) {
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());

        final int variableHeaderBufferSize = 2 + propertiesBuf.readableBytes();
        int payloadBufferSize = 0;

        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        MqttMessageIdVariableHeader variableHeader = message.variableHeader();
        MqttUnsubscribePayload payload = message.payload();

        for (String topicName : payload.topics()) {
            byte[] topicNameBytes = encodeStringUtf8(topicName);
            payloadBufferSize += 2 + topicNameBytes.length;
        }

        int variablePartSize = variableHeaderBufferSize + payloadBufferSize;
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        writeVariableLengthInt(buf, variablePartSize);

        // Variable Header
        int messageId = variableHeader.messageId();
        buf.writeShort(messageId);
        buf.writeBytes(propertiesBuf);

        // Payload
        for (String topicName : payload.topics()) {
            byte[] topicNameBytes = encodeStringUtf8(topicName);
            buf.writeShort(topicNameBytes.length);
            buf.writeBytes(topicNameBytes, 0, topicNameBytes.length);
        }

        return buf;
    }

    private ByteBuf encodeSubAckMessage(
            ByteBufAllocator byteBufAllocator,
            MqttSubAckMessage message) {
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());
        int variableHeaderBufferSize = 2 + propertiesBuf.readableBytes();
        int payloadBufferSize = message.payload().grantedQoSLevels().size();
        int variablePartSize = variableHeaderBufferSize + payloadBufferSize;
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);
        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(message.fixedHeader()));
        writeVariableLengthInt(buf, variablePartSize);
        buf.writeShort(message.variableHeader().messageId());
        buf.writeBytes(propertiesBuf);
        for (int qos : message.payload().grantedQoSLevels()) {
            buf.writeByte(qos);
        }

        return buf;
    }

    private ByteBuf encodeUnsubAckMessage(
            ByteBufAllocator byteBufAllocator,
            MqttUnsubAckMessage message) {
        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());
        int variableHeaderBufferSize = 2 + propertiesBuf.readableBytes();
        int payloadBufferSize = message.payload().unsubscribeReasonCodes().size();
        int variablePartSize = variableHeaderBufferSize + payloadBufferSize;
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);
        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(message.fixedHeader()));
        writeVariableLengthInt(buf, variablePartSize);
        buf.writeShort(message.variableHeader().messageId());
        buf.writeBytes(propertiesBuf);

        for (Short reasonCode : message.payload().unsubscribeReasonCodes()) {
            buf.writeByte(reasonCode);
        }

        return buf;
    }

    private ByteBuf encodePublishMessage(
            ByteBufAllocator byteBufAllocator,
            MqttPublishMessage message) {
        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        MqttPublishVariableHeader variableHeader = message.variableHeader();
        ByteBuf payload = message.payload().duplicate();

        String topicName = variableHeader.topicName();
        byte[] topicNameBytes = encodeStringUtf8(topicName);

        ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, message.variableHeader().properties());

        int variableHeaderBufferSize = 2 + topicNameBytes.length +
                (mqttFixedHeader.qosLevel().value() > 0 ? 2 : 0) + propertiesBuf.readableBytes();
        int payloadBufferSize = payload.readableBytes();
        int variablePartSize = variableHeaderBufferSize + payloadBufferSize;
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variablePartSize);

        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variablePartSize);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        writeVariableLengthInt(buf, variablePartSize);
        buf.writeShort(topicNameBytes.length);
        buf.writeBytes(topicNameBytes);
        if (mqttFixedHeader.qosLevel().value() > 0) {
            buf.writeShort(variableHeader.packetId());
        }
        buf.writeBytes(propertiesBuf);
        buf.writeBytes(payload);

        return buf;
    }

    private ByteBuf encodePubReplyMessage(ByteBufAllocator byteBufAllocator,
                                                 MqttMessage message) {
        if (message.variableHeader() instanceof MqttPubReplyMessageVariableHeader) {
            MqttFixedHeader mqttFixedHeader = message.fixedHeader();
            MqttPubReplyMessageVariableHeader variableHeader =
                    (MqttPubReplyMessageVariableHeader) message.variableHeader();
            int msgId = variableHeader.messageId();

            ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, variableHeader.properties());

            final int reasonLength = mqttVersion() == MqttVersion.MQTT_5 ? 1 : 0;

            final int variableHeaderBufferSize = 2 + reasonLength + propertiesBuf.readableBytes();
            final int fixedHeaderBufferSize = 1 + getVariableLengthInt(variableHeaderBufferSize);
            ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variableHeaderBufferSize);
            buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
            writeVariableLengthInt(buf, variableHeaderBufferSize);
            buf.writeShort(msgId);
            if (mqttVersion() == MqttVersion.MQTT_5) {
                buf.writeByte(variableHeader.reasonCode());
            }
            buf.writeBytes(propertiesBuf);

            return buf;
        } else {
            return encodeMessageWithOnlySingleByteFixedHeaderAndMessageId(byteBufAllocator, message);
        }
    }

    private static ByteBuf encodeMessageWithOnlySingleByteFixedHeaderAndMessageId(
            ByteBufAllocator byteBufAllocator,
            MqttMessage message) {
        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        MqttMessageIdVariableHeader variableHeader = (MqttMessageIdVariableHeader) message.variableHeader();
        int msgId = variableHeader.messageId();

        int variableHeaderBufferSize = 2; // variable part only has a message id
        int fixedHeaderBufferSize = 1 + getVariableLengthInt(variableHeaderBufferSize);
        ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variableHeaderBufferSize);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        writeVariableLengthInt(buf, variableHeaderBufferSize);
        buf.writeShort(msgId);

        return buf;
    }

    private ByteBuf encodeReasonCodePlusPropertiesMessage(ByteBufAllocator byteBufAllocator,
                                          MqttMessage message) {
        if (message.variableHeader() instanceof MqttReasonCodeAndPropertiesVariableHeader) {
            MqttFixedHeader mqttFixedHeader = message.fixedHeader();
            MqttReasonCodeAndPropertiesVariableHeader variableHeader =
                    (MqttReasonCodeAndPropertiesVariableHeader) message.variableHeader();

            ByteBuf propertiesBuf = encodePropertiesIfNeeded(byteBufAllocator, variableHeader.properties());

            final int reasonLength = mqttVersion() == MqttVersion.MQTT_5 ? 1 : 0;

            final int variableHeaderBufferSize = reasonLength + propertiesBuf.readableBytes();
            final int fixedHeaderBufferSize = 1 + getVariableLengthInt(variableHeaderBufferSize);
            ByteBuf buf = byteBufAllocator.buffer(fixedHeaderBufferSize + variableHeaderBufferSize);
            buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
            writeVariableLengthInt(buf, variableHeaderBufferSize);
            if (mqttVersion() == MqttVersion.MQTT_5) {
                buf.writeByte(variableHeader.reasonCode());
            }
            buf.writeBytes(propertiesBuf);

            return buf;
        } else {
            return encodeMessageWithOnlySingleByteFixedHeader(byteBufAllocator, message);
        }
    }

    private static ByteBuf encodeMessageWithOnlySingleByteFixedHeader(
            ByteBufAllocator byteBufAllocator,
            MqttMessage message) {
        MqttFixedHeader mqttFixedHeader = message.fixedHeader();
        ByteBuf buf = byteBufAllocator.buffer(2);
        buf.writeByte(getFixedHeaderByte1(mqttFixedHeader));
        buf.writeByte(0);

        return buf;
    }

    private ByteBuf encodePropertiesIfNeeded(ByteBufAllocator byteBufAllocator,
                                             MqttProperties mqttProperties) {
        if (mqttVersion() == MqttVersion.MQTT_5) {
            return encodeProperties(byteBufAllocator, mqttProperties);
        } else {
            return new EmptyByteBuf(byteBufAllocator);
        }
    }

    private static ByteBuf encodeProperties(ByteBufAllocator byteBufAllocator,
                                            MqttProperties mqttProperties) {
        ByteBuf propertiesHeaderBuf = byteBufAllocator.buffer();
        // encode also the Properties part
        ByteBuf propertiesBuf = byteBufAllocator.buffer();
        for (MqttProperties.MqttProperty property : mqttProperties.listAll()) {
            writeVariableLengthInt(propertiesBuf, property.propertyId);
            switch (MqttProperties.MqttPropertyType.valueOf(property.propertyId)) {
                case PAYLOAD_FORMAT_INDICATOR:
                case REQUEST_PROBLEM_INFORMATION:
                case REQUEST_RESPONSE_INFORMATION:
                case MAXIMUM_QOS:
                case RETAIN_AVAILABLE:
                case WILDCARD_SUBSCRIPTION_AVAILABLE:
                case SUBSCRIPTION_IDENTIFIER_AVAILABLE:
                case SHARED_SUBSCRIPTION_AVAILABLE:
                    final byte bytePropValue = ((MqttProperties.IntegerProperty) property).value.byteValue();
                    propertiesBuf.writeByte(bytePropValue);
                    break;
                case SERVER_KEEP_ALIVE:
                case RECEIVE_MAXIMUM:
                case TOPIC_ALIAS_MAXIMUM:
                case TOPIC_ALIAS:
                    final short twoBytesInPropValue = ((MqttProperties.IntegerProperty) property).value.shortValue();
                    propertiesBuf.writeShort(twoBytesInPropValue);
                    break;
                case PUBLICATION_EXPIRY_INTERVAL:
                case SESSION_EXPIRY_INTERVAL:
                case WILL_DELAY_INTERVAL:
                case MAXIMUM_PACKET_SIZE:
                    final int fourBytesIntPropValue = ((MqttProperties.IntegerProperty) property).value;
                    propertiesBuf.writeInt(fourBytesIntPropValue);
                    break;
                case SUBSCRIPTION_IDENTIFIER:
                    final int vbi = ((MqttProperties.IntegerProperty) property).value;
                    writeVariableLengthInt(propertiesBuf, vbi);
                    break;
                case CONTENT_TYPE:
                case RESPONSE_TOPIC:
                case ASSIGNED_CLIENT_IDENTIFIER:
                case AUTHENTICATION_METHOD:
                case RESPONSE_INFORMATION:
                case SERVER_REFERENCE:
                case REASON_STRING:
                case USER_PROPERTY:
                    final String userPropKey = ((MqttProperties.StringPairProperty) property).key;
                    final String userPropValue = ((MqttProperties.StringPairProperty) property).value;
                    writeUTF8String(propertiesBuf, userPropKey);
                    writeUTF8String(propertiesBuf, userPropValue);
                    break;
                case CORRELATION_DATA:
                case AUTHENTICATION_DATA:
                    final byte[] binaryPropValue = ((MqttProperties.BinaryProperty) property).value;
                    propertiesBuf.writeShort(binaryPropValue.length);
                    propertiesBuf.writeBytes(binaryPropValue, 0, binaryPropValue.length);
                    break;
            }
            writeVariableLengthInt(propertiesHeaderBuf, propertiesBuf.readableBytes());
            propertiesHeaderBuf.writeBytes(propertiesBuf);
        }

        return propertiesHeaderBuf;
    }

    private static int getFixedHeaderByte1(MqttFixedHeader header) {
        int ret = 0;
        ret |= header.messageType().value() << 4;
        if (header.isDup()) {
            ret |= 0x08;
        }
        ret |= header.qosLevel().value() << 1;
        if (header.isRetain()) {
            ret |= 0x01;
        }
        return ret;
    }

    private static void writeVariableLengthInt(ByteBuf buf, int num) {
        do {
            int digit = num % 128;
            num /= 128;
            if (num > 0) {
                digit |= 0x80;
            }
            buf.writeByte(digit);
        } while (num > 0);
    }

    static void writeUTF8String(ByteBuf buf, String s) {
        byte[] sBytes = encodeStringUtf8(s);
        buf.writeShort(sBytes.length);
        buf.writeBytes(sBytes, 0, sBytes.length);
    }

    private static int getVariableLengthInt(int num) {
        int count = 0;
        do {
            num /= 128;
            count++;
        } while (num > 0);
        return count;
    }

    private static byte[] encodeStringUtf8(String s) {
        return s.getBytes(CharsetUtil.UTF_8);
    }
}
