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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.TaskManagerOptionsInternal;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.entrypoint.FlinkParseException;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.externalresource.ExternalResourceUtils;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.io.network.partition.TaskExecutorPartitionTrackerImpl;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalException;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.taskmanager.MemoryLogger;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.StringUtils;
import org.apache.flink.util.TaskManagerExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.UndeclaredThrowableException;
import java.net.InetAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is the executable entry point for the task manager in yarn or standalone mode. It
 * constructs the related components (network, I/O manager, memory manager, RPC service, HA service)
 * and starts them.
 */
public class TaskManagerRunner implements FatalErrorHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TaskManagerRunner.class);

    private static final long FATAL_ERROR_SHUTDOWN_TIMEOUT_MS = 10000L;

    private static final int SUCCESS_EXIT_CODE = 0;
    @VisibleForTesting static final int FAILURE_EXIT_CODE = 1;

    private final Object lock = new Object();

    private final Configuration configuration;

    private final ResourceID resourceId;

    private final Time timeout;

    private final RpcService rpcService;

    private final HighAvailabilityServices highAvailabilityServices;

    private final MetricRegistryImpl metricRegistry;

    private final BlobCacheService blobCacheService;

    /** Executor used to run future callbacks. */
    private final ExecutorService executor;

    private final TaskExecutorService taskExecutorService;

    private final CompletableFuture<Result> terminationFuture;

    private boolean shutdown;

    public TaskManagerRunner(
            Configuration configuration,
            PluginManager pluginManager,
            TaskExecutorServiceFactory taskExecutorServiceFactory)
            throws Exception {
        this.configuration = checkNotNull(configuration);

        timeout = AkkaUtils.getTimeoutAsTime(configuration);

        /***************************************************************************************
         * TODO
         *  初始化进行回调处理的 线程池
         *  future.xxx(() -> xxxxx(),exceutor)
         */
        this.executor =
                java.util.concurrent.Executors.newScheduledThreadPool(
                        Hardware.getNumberCPUCores(),
                        new ExecutorThreadFactory("taskmanager-future"));

        /***************************************************************************************
         * TODO HA 服务：ZookeeperHaServices
         *  提高可用性所需的所有服务的访问注册，分布式计数器和领导人选举
         *  已经解析了 Flink-conf.yaml ，zookeeper
         *
         */
        highAvailabilityServices =
                HighAvailabilityServicesUtils.createHighAvailabilityServices(
                        configuration,
                        executor,
                        HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);

        JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

        /***************************************************************************************
         * TODO
         *  初始化 RpcService
         *  RpcServer 内部细节和主节点的启动方式一致
         */
        rpcService = createRpcService(configuration, highAvailabilityServices);

        this.resourceId =
                getTaskManagerResourceID(
                        configuration, rpcService.getAddress(), rpcService.getPort());

        /***************************************************************************************
         * TODO
         *  初始化 HeartbeatServices
         *  1、TaskExecutor 这个组件是一定会启动的
         *  2、JobMaster JobLeader Flink Job 的主控程序，类似于Spark的 Driver
         *  这两个组件，都需要与 ResourceManager 维持心跳
         *  将来不管有多少个需要发送心跳的组件，都可以通过 heartbeatServices 来创建对应的心跳组件
         */
        HeartbeatServices heartbeatServices = HeartbeatServices.fromConfiguration(configuration);

        /***************************************************************************************
         * TODO  性能监控
         *  Flink 集群监控
         *
         */
        metricRegistry =
                new MetricRegistryImpl(
                        MetricRegistryConfiguration.fromConfiguration(configuration),
                        ReporterSetup.fromConfiguration(configuration, pluginManager));

        final RpcService metricQueryServiceRpcService =
                MetricUtils.startRemoteMetricsRpcService(configuration, rpcService.getAddress());
        metricRegistry.startQueryService(metricQueryServiceRpcService, resourceId);

        /***************************************************************************************
         * TODO 初始化 BlobCacheService
         *  其实内部就是启动两个定时任务，用来定时执行检查，删除过期的 Job 的资源文姬爱你。
         *  通过 引用计数（RefCount）的方式，来判断是否文件过期
         *  1、主节点其实启动一个BlobServer
         *  2、从节点：BlobCacheService
         *  其实有两种具体的实现支撑
         *  1、永久的
         *  2、临时的
         *
         */
        blobCacheService =
                new BlobCacheService(
                        configuration, highAvailabilityServices.createBlobStore(), null);

        // TODO: 5/12/2021  提供外部资源信息
        final ExternalResourceInfoProvider externalResourceInfoProvider =
                ExternalResourceUtils.createStaticExternalResourceInfoProvider(
                        ExternalResourceUtils.getExternalResourceAmountMap(configuration),
                        ExternalResourceUtils.externalResourceDriversFromConfig(
                                configuration, pluginManager));

        /***************************************************************************************
         * TODO
         *  1、负责创建 TaskExecutor ，负责多个任务 Task 的运行
         */
        taskExecutorService =
                taskExecutorServiceFactory.createTaskExecutor(
                        this.configuration,
                        this.resourceId,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        metricRegistry,
                        blobCacheService,
                        false,
                        externalResourceInfoProvider,
                        this);

        this.terminationFuture = new CompletableFuture<>();
        this.shutdown = false;
        handleUnexpectedTaskExecutorServiceTermination();

        MemoryLogger.startIfConfigured(
                LOG, configuration, terminationFuture.thenAccept(ignored -> {}));
    }

    private void handleUnexpectedTaskExecutorServiceTermination() {
        taskExecutorService
                .getTerminationFuture()
                .whenComplete(
                        (unused, throwable) -> {
                            synchronized (lock) {
                                if (!shutdown) {
                                    onFatalError(
                                            new FlinkException(
                                                    "Unexpected termination of the TaskExecutor.",
                                                    throwable));
                                }
                            }
                        });
    }

    // --------------------------------------------------------------------------------------------
    //  Lifecycle management
    // --------------------------------------------------------------------------------------------

    public void start() throws Exception {
        taskExecutorService.start();
    }

    public void close() throws Exception {
        try {
            closeAsync().get();
        } catch (ExecutionException e) {
            ExceptionUtils.rethrowException(ExceptionUtils.stripExecutionException(e));
        }
    }

    public CompletableFuture<Result> closeAsync() {
        return closeAsync(Result.SUCCESS);
    }

    private CompletableFuture<Result> closeAsync(Result terminationResult) {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;

                final CompletableFuture<Void> taskManagerTerminationFuture =
                        taskExecutorService.closeAsync();

                final CompletableFuture<Void> serviceTerminationFuture =
                        FutureUtils.composeAfterwards(
                                taskManagerTerminationFuture, this::shutDownServices);

                serviceTerminationFuture.whenComplete(
                        (Void ignored, Throwable throwable) -> {
                            if (throwable != null) {
                                terminationFuture.completeExceptionally(throwable);
                            } else {
                                terminationFuture.complete(terminationResult);
                            }
                        });
            }
        }

        return terminationFuture;
    }

    private CompletableFuture<Void> shutDownServices() {
        synchronized (lock) {
            Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);
            Exception exception = null;

            try {
                JMXService.stopInstance();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                blobCacheService.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                metricRegistry.shutdown();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            try {
                highAvailabilityServices.close();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }

            terminationFutures.add(rpcService.stopService());

            terminationFutures.add(
                    ExecutorUtils.nonBlockingShutdown(
                            timeout.toMilliseconds(), TimeUnit.MILLISECONDS, executor));

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    // export the termination future for caller to know it is terminated
    public CompletableFuture<Result> getTerminationFuture() {
        return terminationFuture;
    }

    // --------------------------------------------------------------------------------------------
    //  FatalErrorHandler methods
    // --------------------------------------------------------------------------------------------

    @Override
    public void onFatalError(Throwable exception) {
        TaskManagerExceptionUtils.tryEnrichTaskManagerError(exception);
        LOG.error(
                "Fatal error occurred while executing the TaskManager. Shutting it down...",
                exception);

        // In case of the Metaspace OutOfMemoryError, we expect that the graceful shutdown is
        // possible,
        // as it does not usually require more class loading to fail again with the Metaspace
        // OutOfMemoryError.
        if (ExceptionUtils.isJvmFatalOrOutOfMemoryError(exception)
                && !ExceptionUtils.isMetaspaceOutOfMemoryError(exception)) {
            terminateJVM();
        } else {
            closeAsync(Result.FAILURE);

            FutureUtils.orTimeout(
                    terminationFuture, FATAL_ERROR_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        }
    }

    private void terminateJVM() {
        System.exit(FAILURE_EXIT_CODE);
    }


    /***************************************************************************************
     * TODO
     *  TaskManager 启动入口
     */
    // --------------------------------------------------------------------------------------------
    //  Static entry point
    // --------------------------------------------------------------------------------------------

    public static void main(String[] args) throws Exception {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(LOG, "TaskManager", args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        long maxOpenFileHandles = EnvironmentInformation.getOpenFileHandlesLimit();

        if (maxOpenFileHandles != -1L) {
            LOG.info("Maximum number of open file descriptors is {}.", maxOpenFileHandles);
        } else {
            LOG.info("Cannot determine the maximum number of open file descriptors");
        }

        /***************************************************************************************
         * TODO
         *  启动入口
         *  ResourceID : Flink 集群：主节点 从节点 每一个节点都有一个全局唯一的ID
         */
        runTaskManagerProcessSecurely(args);
    }

    public static Configuration loadConfiguration(String[] args) throws FlinkParseException {

        /***************************************************************************************
         * TODO
         *  解析 main 方法参数和 flink-conf.yaml 配置信息
         */
        return ConfigurationParserUtils.loadCommonConfiguration(
                args, TaskManagerRunner.class.getSimpleName());
    }

    public static int runTaskManager(Configuration configuration, PluginManager pluginManager)
            throws Exception {
        final TaskManagerRunner taskManagerRunner;

        try {
            /***************************************************************************************
             * TODO  构建TaskManager 实例
             *  TaskMangerRunner 是 standalone 模式下 TaskManager 的可执行入口点
             *  它构造相关组件（netWork,I/O manager,memory manger,Rpc service ,HA service） 并启动他们
             *
             */
            taskManagerRunner =
                    new TaskManagerRunner(
                            configuration,
                            pluginManager,
                            TaskManagerRunner::createTaskExecutorService);

            // TODO: 12/12/2021 启动 TaskManagerRunner 
            taskManagerRunner.start();
        } catch (Exception exception) {
            throw new FlinkException("Failed to start the TaskManagerRunner.", exception);
        }

        try {
            return taskManagerRunner.getTerminationFuture().get().getExitCode();
        } catch (Throwable t) {
            throw new FlinkException(
                    "Unexpected failure during runtime of TaskManagerRunner.",
                    ExceptionUtils.stripExecutionException(t));
        }
    }

    public static void runTaskManagerProcessSecurely(String[] args) {
        Configuration configuration = null;

        try {
            /***************************************************************************************
             * TODO
             *  加载配置信息
             *  解析 main 方法参数和 flink-conf.yaml 配置信息
             */
            configuration = loadConfiguration(args);
        } catch (FlinkParseException fpe) {
            LOG.error("Could not load the configuration.", fpe);
            System.exit(FAILURE_EXIT_CODE);
        }

        /***************************************************************************************
         * TODO 启动TaskManager
         *
         */
        runTaskManagerProcessSecurely(checkNotNull(configuration));
    }

    public static void runTaskManagerProcessSecurely(Configuration configuration) {
        replaceGracefulExitWithHaltIfConfigured(configuration);

        /***************************************************************************************
         * TODO
         *  初始化插件
         *  主节点启动的时候，也是启动了
         */
        final PluginManager pluginManager =
                PluginUtils.createPluginManagerFromRootFolder(configuration);

        // TODO: 5/12/2021  初始化文件系统
        FileSystem.initialize(configuration, pluginManager);

        int exitCode;
        Throwable throwable = null;

        try {
            SecurityUtils.install(new SecurityConfiguration(configuration));

            /***************************************************************************************
             * TODO
             *  包装启动
             */
            exitCode =
                    SecurityUtils.getInstalledContext()
                            // TODO: 5/12/2021  通过一个线程启动 TaskManager
                            .runSecured(() -> runTaskManager(configuration, pluginManager));
        } catch (Throwable t) {
            throwable = ExceptionUtils.stripException(t, UndeclaredThrowableException.class);
            exitCode = FAILURE_EXIT_CODE;
        }

        if (throwable != null) {
            LOG.error("Terminating TaskManagerRunner with exit code {}.", exitCode, throwable);
        } else {
            LOG.info("Terminating TaskManagerRunner with exit code {}.", exitCode);
        }

        System.exit(exitCode);
    }

    // --------------------------------------------------------------------------------------------
    //  Static utilities
    // --------------------------------------------------------------------------------------------

    public static TaskExecutorService createTaskExecutorService(
            Configuration configuration,
            ResourceID resourceID,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            MetricRegistry metricRegistry,
            BlobCacheService blobCacheService,
            boolean localCommunicationOnly,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        final TaskExecutor taskExecutor =
                startTaskManager(
                        configuration,
                        resourceID,
                        rpcService,
                        highAvailabilityServices,
                        heartbeatServices,
                        metricRegistry,
                        blobCacheService,
                        localCommunicationOnly,
                        externalResourceInfoProvider,
                        fatalErrorHandler);

        return TaskExecutorToServiceAdapter.createFor(taskExecutor);
    }

    /***************************************************************************************
     * TODO
     *  1. 方法名称 startTaskManager
     *  2. 方法返回值：TaskExecutor
     *  启动TaskManager 最重要的事 就是 启动 TaskExecutor
     */
    public static TaskExecutor startTaskManager(
            Configuration configuration,
            ResourceID resourceID,
            RpcService rpcService,
            HighAvailabilityServices highAvailabilityServices,
            HeartbeatServices heartbeatServices,
            MetricRegistry metricRegistry,
            BlobCacheService blobCacheService,
            boolean localCommunicationOnly,
            ExternalResourceInfoProvider externalResourceInfoProvider,
            FatalErrorHandler fatalErrorHandler)
            throws Exception {

        checkNotNull(configuration);
        checkNotNull(resourceID);
        checkNotNull(rpcService);
        checkNotNull(highAvailabilityServices);

        LOG.info("Starting TaskManager with ResourceID: {}", resourceID.getStringWithMetadata());

        String externalAddress = rpcService.getAddress();

        /***************************************************************************************
         * TODO 获取资源定义对象
         *  一台真实的物理节点，到底有哪些资源（cpucore，memory，network）
         *  HardwareDescription 硬件抽象
         *  1、配置：重点是来源于配置：taskManager.slots = ?
         *  2、默认获取
         *  作用： 将来这个 TaskExecutor 在进行注册的时候，会将当前节点的资源，汇报给 RM
         *
         */
        final TaskExecutorResourceSpec taskExecutorResourceSpec =
                TaskExecutorResourceUtils.resourceSpecFromConfig(configuration);

        // TODO: 5/12/2021  taskManagerServicesConfiguration
        TaskManagerServicesConfiguration taskManagerServicesConfiguration =
                TaskManagerServicesConfiguration.fromConfiguration(
                        configuration,
                        resourceID,
                        externalAddress,
                        localCommunicationOnly,
                        taskExecutorResourceSpec);

        Tuple2<TaskManagerMetricGroup, MetricGroup> taskManagerMetricGroup =
                MetricUtils.instantiateTaskManagerMetricGroup(
                        metricRegistry,
                        externalAddress,
                        resourceID,
                        taskManagerServicesConfiguration.getSystemResourceMetricsProbingInterval());

        /***************************************************************************************
         * TODO 初始化 ioExecutor
         *
         */
        final ExecutorService ioExecutor =
                Executors.newFixedThreadPool(
                        taskManagerServicesConfiguration.getNumIoThreads(),
                        new ExecutorThreadFactory("flink-taskexecutor-io"));

        /***************************************************************************************
         * TODO 重点
         *  里面初始化了很多很多的 TaskManager 在运行过程中 需要的服务
         *  在这 TaskManager 启动之前，已经初始化了一些服务组件，基础服务。
         *  在这里面创建的服务，就是 TaskManager 在运行过程中，真正需要的用来对外提供服务的 各种服务组件。
         */
        TaskManagerServices taskManagerServices =
                TaskManagerServices.fromConfiguration(
                        taskManagerServicesConfiguration,
                        blobCacheService.getPermanentBlobService(),
                        taskManagerMetricGroup.f1,
                        ioExecutor,
                        fatalErrorHandler);


        MetricUtils.instantiateFlinkMemoryMetricGroup(
                taskManagerMetricGroup.f1,
                taskManagerServices.getTaskSlotTable(),
                taskManagerServices::getManagedMemorySize);

        TaskManagerConfiguration taskManagerConfiguration =
                TaskManagerConfiguration.fromConfiguration(
                        configuration, taskExecutorResourceSpec, externalAddress);

        String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

        /***************************************************************************************
         * TODO  创建 TaskExecutor 实例
         *  内部会创建两个重要的心跳管理器：
         *  1. JobManagerHeartManager
         *  2.ResourceManagerHeartbeatManager
         *  -
         *  这里才是 初始化 TaskManager 最重要的地方！
         *
         */
        return new TaskExecutor(
                rpcService,
                taskManagerConfiguration,
                highAvailabilityServices,
                taskManagerServices,
                externalResourceInfoProvider,
                heartbeatServices,
                taskManagerMetricGroup.f0,
                metricQueryServiceAddress,
                blobCacheService,
                fatalErrorHandler,
                new TaskExecutorPartitionTrackerImpl(taskManagerServices.getShuffleEnvironment()),
                createBackPressureSampleService(configuration, rpcService.getScheduledExecutor()));
    }

    static BackPressureSampleService createBackPressureSampleService(
            Configuration configuration, ScheduledExecutor scheduledExecutor) {
        return new BackPressureSampleService(
                configuration.getInteger(WebOptions.BACKPRESSURE_NUM_SAMPLES),
                Time.milliseconds(configuration.getInteger(WebOptions.BACKPRESSURE_DELAY)),
                scheduledExecutor);
    }

    /**
     * Create a RPC service for the task manager.
     *
     * @param configuration The configuration for the TaskManager.
     * @param haServices to use for the task manager hostname retrieval
     */
    @VisibleForTesting
    static RpcService createRpcService(
            final Configuration configuration, final HighAvailabilityServices haServices)
            throws Exception {

        checkNotNull(configuration);
        checkNotNull(haServices);

        return AkkaRpcServiceUtils.createRemoteRpcService(
                configuration,
                determineTaskManagerBindAddress(configuration, haServices),
                configuration.getString(TaskManagerOptions.RPC_PORT),
                configuration.getString(TaskManagerOptions.BIND_HOST),
                configuration.getOptional(TaskManagerOptions.RPC_BIND_PORT));
    }

    private static String determineTaskManagerBindAddress(
            final Configuration configuration, final HighAvailabilityServices haServices)
            throws Exception {

        final String configuredTaskManagerHostname =
                configuration.getString(TaskManagerOptions.HOST);

        if (configuredTaskManagerHostname != null) {
            LOG.info(
                    "Using configured hostname/address for TaskManager: {}.",
                    configuredTaskManagerHostname);
            return configuredTaskManagerHostname;
        } else {
            return determineTaskManagerBindAddressByConnectingToResourceManager(
                    configuration, haServices);
        }
    }

    private static String determineTaskManagerBindAddressByConnectingToResourceManager(
            final Configuration configuration, final HighAvailabilityServices haServices)
            throws LeaderRetrievalException {

        final Duration lookupTimeout = AkkaUtils.getLookupTimeout(configuration);

        final InetAddress taskManagerAddress =
                LeaderRetrievalUtils.findConnectingAddress(
                        haServices.getResourceManagerLeaderRetriever(), lookupTimeout);

        LOG.info(
                "TaskManager will use hostname/address '{}' ({}) for communication.",
                taskManagerAddress.getHostName(),
                taskManagerAddress.getHostAddress());

        HostBindPolicy bindPolicy =
                HostBindPolicy.fromString(
                        configuration.getString(TaskManagerOptions.HOST_BIND_POLICY));
        return bindPolicy == HostBindPolicy.IP
                ? taskManagerAddress.getHostAddress()
                : taskManagerAddress.getHostName();
    }

    @VisibleForTesting
    static ResourceID getTaskManagerResourceID(Configuration config, String rpcAddress, int rpcPort)
            throws Exception {
        return new ResourceID(
                config.getString(
                        TaskManagerOptions.TASK_MANAGER_RESOURCE_ID,
                        StringUtils.isNullOrWhitespaceOnly(rpcAddress)
                                ? InetAddress.getLocalHost().getHostName()
                                        + "-"
                                        + new AbstractID().toString().substring(0, 6)
                                : rpcAddress
                                        + ":"
                                        + rpcPort
                                        + "-"
                                        + new AbstractID().toString().substring(0, 6)),
                config.getString(TaskManagerOptionsInternal.TASK_MANAGER_RESOURCE_ID_METADATA, ""));
    }

    /** Factory for {@link TaskExecutor}. */
    public interface TaskExecutorServiceFactory {
        TaskExecutorService createTaskExecutor(
                Configuration configuration,
                ResourceID resourceID,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                MetricRegistry metricRegistry,
                BlobCacheService blobCacheService,
                boolean localCommunicationOnly,
                ExternalResourceInfoProvider externalResourceInfoProvider,
                FatalErrorHandler fatalErrorHandler)
                throws Exception;
    }

    public interface TaskExecutorService extends AutoCloseableAsync {
        void start();

        CompletableFuture<Void> getTerminationFuture();
    }

    public enum Result {
        SUCCESS(SUCCESS_EXIT_CODE),
        FAILURE(FAILURE_EXIT_CODE);

        private final int exitCode;

        Result(int exitCode) {
            this.exitCode = exitCode;
        }

        public int getExitCode() {
            return exitCode;
        }
    }
}
