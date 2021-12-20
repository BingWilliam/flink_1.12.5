/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * {@link HeartbeatManager} implementation which regularly requests a heartbeat response from its
 * monitored {@link HeartbeatTarget}. The heartbeat period is configurable.
 *
 * @param <I> Type of the incoming heartbeat payload
 * @param <O> Type of the outgoing heartbeat payload
 */
public class HeartbeatManagerSenderImpl<I, O> extends HeartbeatManagerImpl<I, O>
        implements Runnable {

    private final long heartbeatPeriod;

    HeartbeatManagerSenderImpl(
            long heartbeatPeriod,
            long heartbeatTimeout,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log) {
        this(
                heartbeatPeriod,
                heartbeatTimeout,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                new HeartbeatMonitorImpl.Factory<>());
    }

    HeartbeatManagerSenderImpl(
            long heartbeatPeriod,
            long heartbeatTimeout,
            ResourceID ownResourceID,
            HeartbeatListener<I, O> heartbeatListener,
            ScheduledExecutor mainThreadExecutor,
            Logger log,
            HeartbeatMonitor.Factory<O> heartbeatMonitorFactory) {
        super(
                heartbeatTimeout,
                ownResourceID,
                heartbeatListener,
                mainThreadExecutor,
                log,
                heartbeatMonitorFactory);

        this.heartbeatPeriod = heartbeatPeriod;
        /***************************************************************************************
         * TODO
         *  启动定时任务
         *  执行的就是当前类的 run() 方法
         */
        mainThreadExecutor.schedule(this, 0L, TimeUnit.MILLISECONDS);
    }

    @Override
    public void run() {
        /***************************************************************************************
         * TODO
         *  在 Flink 的心跳机制中，跟其他 集群不一样：
         *  1、ResourceManager 发送心跳给从节点 TaskManager
         *  2、从节点接收到心跳之后，返回响应
         */
        // 实现循环
        if (!stopped) {
            /***************************************************************************************
             * TODO 遍历每一个 TaskExecutor 出来 然后发送心跳请求
             *  每一次 TaskExecutor 来 RM 注册的时候，在注册成功后，就会给这个 TaskExecutor 生成一个 Target
             *  最终这个Target 会被封装在 Monitor中。每个TaskExecutor 对应一个唯一的 Monitor ，就保存在 heartbeatTargets map 中
             *  RM 启动的时候，就已经启动了 TaskManagerHeartbeatManager这个组件内部的 HeartbeatManagerSenderImpl 队形
             *  内部通过一种定时调度任务，每10s 调度一次该组件的run 方法
             *  最终的效果：
             *  RM 启动好后，就每隔10s ，向所有的已注册的 TaskExecutor 发送心跳请求，如果发现某一个 TaskExecutor
             *  的上一次心跳时间，距离现在超过 50s 则认为该 TaskExecutor 宕机了。RM 要执行针对这个 TaskExecutor 的注销
             */
            log.debug("Trigger heartbeat request.");
            for (HeartbeatMonitor<O> heartbeatMonitor : getHeartbeatTargets().values()) {
                // TODO ResourceManager 给目标发送（TaskManager 或者 JobManager） 心跳
                requestHeartbeat(heartbeatMonitor);
            }

            /***************************************************************************************
             * TODO 实现循环发送心跳的效果
             *  1、心跳时间：10秒
             *  2、心跳超时时间：50s
             */
            getMainThreadExecutor().schedule(this, heartbeatPeriod, TimeUnit.MILLISECONDS);
        }
    }

    /***************************************************************************************
     * TODO
     *  heartbeatMonitor 如果有从节点返回心跳响应，则会被加入到 HeartBeatMonitor
     */
    private void requestHeartbeat(HeartbeatMonitor<O> heartbeatMonitor) {
        O payload = getHeartbeatListener().retrievePayload(heartbeatMonitor.getHeartbeatTargetId());
        final HeartbeatTarget<O> heartbeatTarget = heartbeatMonitor.getHeartbeatTarget();

        /***************************************************************************************
         * TODO
         *  发送心跳
         *  heartbeatTarget -> 集群中启动的从节点
         */
        heartbeatTarget.requestHeartbeat(getOwnResourceID(), payload);
    }
}
