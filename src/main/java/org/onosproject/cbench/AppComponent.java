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
import org.onlab.util.Tools;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.net.Device;
import org.onosproject.net.DeviceId;
import org.onosproject.net.PortNumber;
import org.onosproject.net.device.DeviceAdminService;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.flow.DefaultFlowRule;
import org.onosproject.net.flow.DefaultTrafficSelector;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRule;
import org.onosproject.net.flow.FlowRuleOperations;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.TrafficSelector;
import org.onosproject.net.flow.TrafficTreatment;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;

import org.onosproject.net.packet.DefaultOutboundPacket;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.OutboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
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

    private DeviceListener deviceListener = new InternalDeviceListener();

    private ApplicationId appId;

    private ArrayList<DeviceId> deviceIdList;

    private static final int PACKET_BUFFER = 5000;

    private static final int BATCH_SIZE = 1000;

    private static final int AVERAGE_COUNT = 5;

    /**
     * PacketIn message handle queue
     */
    private BlockingDeque<PacketContext> handleQueue =
            new LinkedBlockingDeque<>(PACKET_BUFFER);

    /**
     * Single thread executor for PacketIn handling.
     */
    private ExecutorService executorService;

    boolean hasBegin = false;

    boolean hasEnd = false;

    @Activate
    public void activate() {
        appId = coreService.registerApplication("org.onosproject.cbench");

        packetService.addProcessor(processor, PacketProcessor.director(2));
        deviceService.addListener(deviceListener);

        /*for (Device device: deviceService.getAvailableDevices()) {
            DeviceId deviceId = device.id();
            deviceIdList.add(deviceId);
        }
        log.info("available devices: {}", deviceIdList);*/

        executorService = Executors.newSingleThreadExecutor(Tools.groupedThreads("onos/cbench/packetin/handler", "1", log));

        log.info("Started", appId.id());
    }

    @Deactivate
    public void deactivate() {
        flowRuleService.removeFlowRulesById(appId);

        packetService.removeProcessor(processor);
        processor = null;

        deviceService.removeListener(deviceListener);
        deviceListener = null;

        if (executorService != null) {
            log.info("flow rule installer shutdown: {}", executorService);
            executorService.shutdown();
        }

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

            // double check, only executed once
            if (!hasBegin) {
                synchronized (this) {
                    if(!hasBegin) {
                        hasBegin = true;
                        hasEnd = false;
                        deviceIdList = new ArrayList<>();
                        for (Device device: deviceService.getAvailableDevices()) {
                            DeviceId deviceId = device.id();
                            deviceIdList.add(deviceId);
                        }
                        log.info("{} devices: {}", deviceIdList.size(), deviceIdList);

                        executorService.execute(new FlowRuleInstaller(deviceIdList, AVERAGE_COUNT, BATCH_SIZE));
                        log.info("flow rule installer started: {}", executorService);
                    }
                }
            }

            try {
                handleQueue.put(context);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("put packet to queue exception");
            }

            //installRule(deviceIdList, 5);
            //installRule(pkt.receivedFrom().deviceId());

            //packetOut(context, PortNumber.portNumber(2));

            //emit(context, PortNumber.portNumber(2));
        }
    }

    private class FlowRuleInstaller implements Runnable {

        int batchSize;
        int averageCount;
        List<DeviceId> deviceIds;

        public FlowRuleInstaller(List<DeviceId> deviceIds, int averageCount, int batchSize) {
            this.deviceIds = deviceIds;
            this.averageCount = averageCount;
            this.batchSize = batchSize;
        }

        @Override
        public void run() {
            FlowRuleOperations.Builder rules = FlowRuleOperations.builder();

            PortNumber portNumber = PortNumber.portNumber(RandomUtils.nextInt());
            TrafficTreatment treatment = DefaultTrafficTreatment.builder()
                    .setOutput(portNumber).build();

            TrafficSelector.Builder sbuilder;

            int size = 0;
            while (true) {
                try {
                    //wait for a new packetIn message
                    PacketContext packetContext = handleQueue.take();

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
                        size++;
                    }
                    //apply flow rules as a batch
                    if(size >= batchSize) {
                        flowRuleService.apply(rules.build());

                        rules = FlowRuleOperations.builder();
                        portNumber = PortNumber.portNumber(RandomUtils.nextInt());
                        treatment = DefaultTrafficTreatment.builder()
                                .setOutput(portNumber).build();
                        size = 0;
                    }
                } catch (Exception e) {
                    Thread.currentThread().interrupt();
                    // interrupted. gracefully shutting down
                    log.warn("exception occur!");
                    return;
                }

            }
        }

    }

    private class InternalDeviceListener implements DeviceListener {

        @Override
        public void event(DeviceEvent event) {
            switch (event.type()) {
                case DEVICE_ADDED:
                case DEVICE_UPDATED:
                    DeviceId deviceId = event.subject().id();
                    //log.info("device connected: {}", deviceId);
                    break;
                case DEVICE_REMOVED:
                    // double check, only executed once
                    log.info("device disconnected: {}", event.subject().id());
                    if(!hasEnd) {
                        synchronized (this) {
                            if(!hasEnd) {
                                hasBegin = false;
                                hasEnd = true;
                                if (executorService != null) {
                                    log.info("flow rule installer shutdown: {}", executorService);
                                    executorService.shutdown();
                                }
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
        }
    }
}
