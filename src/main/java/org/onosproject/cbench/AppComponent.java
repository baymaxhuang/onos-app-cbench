/*
 * Copyright 2016-present Open Networking Laboratory
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.onosproject.cbench;

import org.apache.commons.lang.math.RandomUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.packet.Ethernet;
import org.onlab.packet.MacAddress;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.*;
import org.onosproject.net.device.DeviceAdminService;
import org.onosproject.net.flow.*;
import org.onosproject.net.packet.*;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Skeletal ONOS application component.
 */
@Component(immediate = true)
public class AppComponent {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceAdminService deviceService;

    private ReactivePacketProcessor processor = new ReactivePacketProcessor();

    private ApplicationId appId;

    private ArrayList<DeviceId> deviceIdList = new ArrayList<>();

    @Activate
    public void activate() {
        appId = coreService.registerApplication("org.onosproject.cbench");

        packetService.addProcessor(processor, PacketProcessor.director(2));

        for (Device device: deviceService.getAvailableDevices()) {
            DeviceId deviceId = device.id();
            deviceIdList.add(deviceId);
        }
        log.info("available devices: {}", deviceIdList);
        log.info("Started", appId.id());
    }

    @Deactivate
    public void deactivate() {
        flowRuleService.removeFlowRulesById(appId);
        packetService.removeProcessor(processor);

        processor = null;
        log.info("Stopped");
    }

    // Sends a packet out the specified port.
    private void packetOut(PacketContext context, PortNumber portNumber) {
        context.treatmentBuilder().setOutput(portNumber);
        context.send();
    }

    private void emit(PacketContext context, PortNumber portNumber) {
        TrafficTreatment treatment = DefaultTrafficTreatment
                .builder()
                .setOutput(portNumber)
                .build();

        Ethernet eth = context.inPacket().parsed();

        OutboundPacket outboundPacket = new DefaultOutboundPacket(
                context.inPacket().receivedFrom().deviceId(), treatment, ByteBuffer.wrap(eth.serialize()));
        packetService.emit(outboundPacket);
    }

    private void installRule(DeviceId deviceId) {
        PortNumber portNumber = PortNumber.portNumber(RandomUtils.nextInt());
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber).build();

        TrafficSelector.Builder sbuilder = DefaultTrafficSelector.builder();

        sbuilder.matchEthSrc(MacAddress.valueOf(RandomUtils.nextInt()))
                .matchEthDst(MacAddress.valueOf((Integer.MAX_VALUE ) * RandomUtils.nextInt()));


        int randomPriority = RandomUtils.nextInt(FlowRule.MAX_PRIORITY);
        FlowRule flowRule = DefaultFlowRule.builder()
                .forDevice(deviceId)
                .withSelector(sbuilder.build())
                .withTreatment(treatment)
                .withPriority(randomPriority)
                .fromApp(appId)
                .makeTemporary(10)
                .build();
        flowRuleService.applyFlowRules(flowRule);
    }

    private void installRule(List<DeviceId> deviceIds, int averageCount) {

        FlowRuleOperations.Builder rules = FlowRuleOperations.builder();

        PortNumber portNumber = PortNumber.portNumber(RandomUtils.nextInt());
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber).build();

        TrafficSelector.Builder sbuilder;

        for (int i = 0; i < averageCount; i++) {
            DeviceId deviceId = deviceIds.get(RandomUtils.nextInt(deviceIds.size()));

            sbuilder = DefaultTrafficSelector.builder();

            sbuilder.matchEthSrc(MacAddress.valueOf(RandomUtils.nextInt()))
                    .matchEthDst(MacAddress.valueOf((Integer.MAX_VALUE ) * RandomUtils.nextInt()));

            int randomPriority = RandomUtils.nextInt(FlowRule.MAX_PRIORITY);
            FlowRule flowRule = DefaultFlowRule.builder()
                    .forDevice(deviceId)
                    .withSelector(sbuilder.build())
                    .withTreatment(treatment)
                    .withPriority(randomPriority)
                    .fromApp(appId)
                    .makeTemporary(10)
                    .build();
            rules.add(flowRule);
        }
        flowRuleService.apply(rules.build());
    }

    private void installRule(List<DeviceId> deviceIds) {

        FlowRuleOperations.Builder rules = FlowRuleOperations.builder();

        PortNumber portNumber = PortNumber.portNumber(RandomUtils.nextInt());
        TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                .setOutput(portNumber).build();

        TrafficSelector.Builder sbuilder;

        for (DeviceId deviceId : deviceIds) {
            sbuilder = DefaultTrafficSelector.builder();

            sbuilder.matchEthSrc(MacAddress.valueOf(RandomUtils.nextInt()))
                    .matchEthDst(MacAddress.valueOf((Integer.MAX_VALUE ) * RandomUtils.nextInt()));

            int randomPriority = RandomUtils.nextInt(FlowRule.MAX_PRIORITY);
            FlowRule flowRule = DefaultFlowRule.builder()
                    .forDevice(deviceId)
                    .withSelector(sbuilder.build())
                    .withTreatment(treatment)
                    .withPriority(randomPriority)
                    .fromApp(appId)
                    .makeTemporary(10)
                    .build();
            rules.add(flowRule);
        }
        flowRuleService.apply(rules.build());
    }

    private class ReactivePacketProcessor implements PacketProcessor {
        @Override
        public void process(PacketContext context) {

            // Stop processing if the packet has been handled, since we
            // can't do any more to it.
            if (context.isHandled()) {
                return;
            }
            InboundPacket pkt = context.inPacket();
            Ethernet ethPkt = pkt.parsed();

            if (ethPkt == null) {
                return;
            }

            installRule(deviceIdList, 5);
            //installRule(pkt.receivedFrom().deviceId());

            //packetOut(context, PortNumber.portNumber(2));

            //emit(context, PortNumber.portNumber(2));
        }
    }

}
