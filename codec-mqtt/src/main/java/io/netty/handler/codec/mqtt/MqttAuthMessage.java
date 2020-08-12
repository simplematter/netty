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

/**
 * MQTT Auth message
 */
public final class MqttAuthMessage extends MqttMessage {

    public MqttAuthMessage(
            MqttFixedHeader mqttFixedHeader,
            MqttReasonCodeAndPropertiesVariableHeader variableHeader) {
        super(mqttFixedHeader, variableHeader);
    }

    @Override
    public MqttReasonCodeAndPropertiesVariableHeader variableHeader() {
        return (MqttReasonCodeAndPropertiesVariableHeader) super.variableHeader();
    }

}
