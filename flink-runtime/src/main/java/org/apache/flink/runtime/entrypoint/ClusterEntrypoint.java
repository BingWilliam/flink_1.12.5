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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JMXServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.ArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.MiniDispatcher;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponent;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.parser.CommandLineParser;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.management.JMXService;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.ReporterSetup;
import org.apache.flink.runtime.metrics.groups.ProcessMetricGroup;
import org.apache.flink.runtime.metrics.util.MetricUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.security.SecurityConfiguration;
import org.apache.flink.runtime.security.SecurityUtils;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.webmonitor.retriever.impl.RpcMetricQueryServiceRetriever;
import org.apache.flink.util.AutoCloseableAsync;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.security.ExitTrappingSecurityManager.replaceGracefulExitWithHaltIfConfigured;

/**
 * Base class for the Flink cluster entry points.
 *
 * <p>Specialization of this class can be used for the session mode and the per-job mode
 */
public abstract class ClusterEntrypoint implements AutoCloseableAsync, FatalErrorHandler {

    public static final ConfigOption<String> EXECUTION_MODE =
            ConfigOptions.key("internal.cluster.execution-mode")
                    .defaultValue(ExecutionMode.NORMAL.toString());

    protected static final Logger LOG = LoggerFactory.getLogger(ClusterEntrypoint.class);

    protected static final int STARTUP_FAILURE_RETURN_CODE = 1;
    protected static final int RUNTIME_FAILURE_RETURN_CODE = 2;

    private static final Time INITIALIZATION_SHUTDOWN_TIMEOUT = Time.seconds(30L);

    /** The lock to guard startup / shutdown / manipulation methods. */
    private final Object lock = new Object();

    private final Configuration configuration;

    private final CompletableFuture<ApplicationStatus> terminationFuture;

    private final AtomicBoolean isShutDown = new AtomicBoolean(false);

    @GuardedBy("lock")
    private DispatcherResourceManagerComponent clusterComponent;

    @GuardedBy("lock")
    private MetricRegistryImpl metricRegistry;

    @GuardedBy("lock")
    private ProcessMetricGroup processMetricGroup;

    @GuardedBy("lock")
    private HighAvailabilityServices haServices;

    @GuardedBy("lock")
    private BlobServer blobServer;

    @GuardedBy("lock")
    private HeartbeatServices heartbeatServices;

    @GuardedBy("lock")
    private RpcService commonRpcService;

    @GuardedBy("lock")
    private ExecutorService ioExecutor;

    private ArchivedExecutionGraphStore archivedExecutionGraphStore;

    private final Thread shutDownHook;

    protected ClusterEntrypoint(Configuration configuration) {
        this.configuration = generateClusterConfiguration(configuration);
        this.terminationFuture = new CompletableFuture<>();

        shutDownHook =
                ShutdownHookUtil.addShutdownHook(
                        () -> this.closeAsync().join(), getClass().getSimpleName(), LOG);
    }

    public CompletableFuture<ApplicationStatus> getTerminationFuture() {
        return terminationFuture;
    }

    public void startCluster() throws ClusterEntrypointException {
        LOG.info("Starting {}.", getClass().getSimpleName());

        try {
            replaceGracefulExitWithHaltIfConfigured(configuration);
            PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            configureFileSystems(configuration, pluginManager);

            SecurityContext securityContext = installSecurityContext(configuration);

            securityContext.runSecured(
                    (Callable<Void>)
                            () -> {
                                runCluster(configuration, pluginManager);

                                return null;
                            });
        } catch (Throwable t) {
            final Throwable strippedThrowable =
                    ExceptionUtils.stripException(t, UndeclaredThrowableException.class);

            try {
                // clean up any partial state
                shutDownAsync(
                                ApplicationStatus.FAILED,
                                ShutdownBehaviour.STOP_APPLICATION,
                                ExceptionUtils.stringifyException(strippedThrowable),
                                false)
                        .get(
                                INITIALIZATION_SHUTDOWN_TIMEOUT.toMilliseconds(),
                                TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                strippedThrowable.addSuppressed(e);
            }

            throw new ClusterEntrypointException(
                    String.format(
                            "Failed to initialize the cluster entrypoint %s.",
                            getClass().getSimpleName()),
                    strippedThrowable);
        }
    }

    private void configureFileSystems(Configuration configuration, PluginManager pluginManager) {
        LOG.info("Install default filesystem.");
        FileSystem.initialize(configuration, pluginManager);
    }

    private SecurityContext installSecurityContext(Configuration configuration) throws Exception {
        LOG.info("Install security context.");

        SecurityUtils.install(new SecurityConfiguration(configuration));

        return SecurityUtils.getInstalledContext();
    }

    private void runCluster(Configuration configuration, PluginManager pluginManager)
            throws Exception {
        synchronized (lock) {
            /***************************
             * TODO 初始化服务，如JobManager 的Akka RPC 服务，HA服务，心跳检查服务，metric service
             *  这些服务都是 Master 节点是使用到的一些服务
             *  1、commonRpcService： 基于Akka 的RpcService 实现。Rpc 服务启动Akka 参与者来接收从RpcGateway 调用Rpc
             *  2、haServices： 提供高可用所需的所有服务的访问注册，分布式计数器和领导人选举
             *  3、blobServer： 负责侦听传入的请求生成线程来处理这些请求。它还负责创建要存储的目录结构 blob 或临时缓存大文件
             *  4、heartbeatServices：提供心跳所需的所有服务。者包括创建心跳接收器和心跳发送者
             *  5、metricRegistry：跟踪所有已注册的Metric，它作为连接 MetricGroup 和 MetricReporter
             *  6、archivedExecutionGraphStore：存储执行图 ExecutionGraph 的可序列化形式
             */
            initializeServices(configuration, pluginManager);

            /*********************************
             * TODO 将 jobManager 地址写入配置
             */
            // write host information into configuration
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());

            /*********************
             * TODO 初始化一个 DispatcherResourceManagerComponentFactory 工厂实例
             *  内部初始化了四大工厂
             *  1、DisPatcherRunnerFactory = DefaultDispatcherRunnerFactory
             *  2、ResourceManagerFactory = StandaloneResourcemanagerFactory
             *  3、RestEndPointFactory(WebMonitorEndPoint的工厂)=SessionRestEndpointFactory
             *  返回值：DefaultDispatcherResourceManagerComponentFactory
             */
            final DispatcherResourceManagerComponentFactory
                    dispatcherResourceManagerComponentFactory =
                            createDispatcherResourceManagerComponentFactory(configuration);

            /**************
             * TODO 启动关键组件：Dispatcher 和 ResourceManager
             *  1、Dispatcher：负责用于接收作业提交，持久化它们，生成要执行的作业管理器任务，并在主任务失败时恢复他们。
             *                 此外，它知道关于Flink 会话集群的状态。负责为这个新提交的作业拉起一个新的JobManager 服务
             *  2、ResourceManager： 负责资源的分配和记账。在整个Flink 集群中只有一个 ResourceManager，资源相关的内部都有这个管理
             *  			   registerJobManager(JobMasterId,ResourceID,String,JobId,Time) 负责注册 jobmaster
             *  			   requestSlot(JobMasterId,SlotRequest,Time)从资源管理器请求一个槽
             *  3、WebMonitorEndPoint：服务于web 前端 Rest 调用的Rest 端点，用于接收客户端发送的执行任务的请求
             */
            clusterComponent =
                    dispatcherResourceManagerComponentFactory.create(
                            configuration,
                            ioExecutor,
                            commonRpcService,
                            haServices,
                            blobServer,
                            heartbeatServices,
                            metricRegistry,
                            archivedExecutionGraphStore,
                            new RpcMetricQueryServiceRetriever(
                                    metricRegistry.getMetricQueryServiceRpcService()),
                            this);

            clusterComponent
                    .getShutDownFuture()
                    .whenComplete(
                            (ApplicationStatus applicationStatus, Throwable throwable) -> {
                                if (throwable != null) {
                                    shutDownAsync(
                                            ApplicationStatus.UNKNOWN,
                                            ShutdownBehaviour.STOP_APPLICATION,
                                            ExceptionUtils.stringifyException(throwable),
                                            false);
                                } else {
                                    // This is the general shutdown path. If a separate more
                                    // specific shutdown was
                                    // already triggered, this will do nothing
                                    shutDownAsync(
                                            applicationStatus,
                                            ShutdownBehaviour.STOP_APPLICATION,
                                            null,
                                            true);
                                }
                            });
        }
    }

    protected void initializeServices(Configuration configuration, PluginManager pluginManager)
            throws Exception {

        LOG.info("Initializing cluster services.");

        synchronized (lock) {
            /*********************
             * TODO 第一步
             *  创建一个 Akka rpc 服务 commonRpcService：基于 Akka 的 RpcService 实现
             *  RPC 服务启动 Akka 参与者来接收从 RpcGateway 调用 RPC
             *  commonRpcService 其实是一个基于 akka 得 actorSystem，其实就是一个 tcp 的 rpc 服务，端口：6123
             *  1、初始化 ActorSystem
             *  2、启动 actor
             */
            commonRpcService =
                    AkkaRpcServiceUtils.createRemoteRpcService(
                            configuration,
                            configuration.getString(JobManagerOptions.ADDRESS),
                            getRPCPortRange(configuration),
                            configuration.getString(JobManagerOptions.BIND_HOST),
                            configuration.getOptional(JobManagerOptions.RPC_BIND_PORT));

            JMXService.startInstance(configuration.getString(JMXServerOptions.JMX_SERVER_PORT));

            // update the configuration used to create the high availability services
            configuration.setString(JobManagerOptions.ADDRESS, commonRpcService.getAddress());
            configuration.setInteger(JobManagerOptions.PORT, commonRpcService.getPort());


            /*******************
             *
             * TODO 第二步
             *  初始化一个ioExecutor
             *  如果当前节点有32个cpu，那么当前这个ioExecutor 启动的线程数量为：128 。（4倍）
             */
            ioExecutor =
                    Executors.newFixedThreadPool(
                            ClusterEntrypointUtils.getPoolSize(configuration),
                            new ExecutorThreadFactory("cluster-io"));
            /*****************
             * TODO 第三步
             *  HA service 相关实现，它的作用有很多，到底使用哪种根据用户的需求来定义
             *  比如：处理ResourceManager 的 leader 选举，Jobmanager leader 选举等；
             *  haServices = ZookeeperHaServices
             */
            haServices = createHaServices(configuration, ioExecutor);

            /********************
             * TODO 第四步：初始化一个 BlobServer
             *  主要管理一些大文件的上传等，比如用户作业的jar包、TM 上创 log 文件等
             *  Blob 是指 二进制大对象也就是英文 Binary Large Object 的缩写
             *
             */
            blobServer = new BlobServer(configuration, haServices.createBlobStore());
            blobServer.start();

            /*********************
             * TODO 第五步
             *  初始化一个心跳服务
             *  在主节点中，其实有很多角色都有心跳服务。那些角色的心跳服务，都是在这个 heartbeatServices 的基础上创建的
             *  这才是真正的心跳服务的提供者
             *  谁需要心跳服务，通过 heartbeatServices 去提供一个实例 HeartBeatImpl，用来完成心跳
             */
            heartbeatServices = createHeartbeatServices(configuration);

            /********************
             * TODO 第六步
             *  metrics（性能监控）相关的服务
             *  1、metriQueryServiceRpcService 也是一个 ActorSystem
             *  2、用来跟踪所有已经注册的 Metric
             */
            metricRegistry = createMetricRegistry(configuration, pluginManager);

            final RpcService metricQueryServiceRpcService =
                    MetricUtils.startRemoteMetricsRpcService(
                            configuration, commonRpcService.getAddress());
            metricRegistry.startQueryService(metricQueryServiceRpcService, null);

            final String hostname = RpcUtils.getHostname(commonRpcService);

            processMetricGroup =
                    MetricUtils.instantiateProcessMetricGroup(
                            metricRegistry,
                            hostname,
                            ConfigurationUtils.getSystemResourceMetricsProbingInterval(
                                    configuration));

            /****************************
             * TODO 第七步：archivedExecutionGraphStore：存储 execution graph 的服务，默认有两种实现
             *  1、MemoryArchivedExecutionGraphStore 主要是在内存中缓存
             *  2、FileArchivedExecutionGraphStore 会持久到文件系统，也会在内存中缓存，默认是基于 File 存储
             *  这些服务都会在前面第二步创建 DispatcherResourceManagerComponent 对象时使用到。
             */
            archivedExecutionGraphStore =
                    createSerializableExecutionGraphStore(
                            configuration, commonRpcService.getScheduledExecutor());
        }
    }

    /**
     * Returns the port range for the common {@link RpcService}.
     *
     * @param configuration to extract the port range from
     * @return Port range for the common {@link RpcService}
     */
    protected String getRPCPortRange(Configuration configuration) {
        if (ZooKeeperUtils.isZooKeeperRecoveryMode(configuration)) {
            return configuration.getString(HighAvailabilityOptions.HA_JOB_MANAGER_PORT_RANGE);
        } else {
            return String.valueOf(configuration.getInteger(JobManagerOptions.PORT));
        }
    }

    protected HighAvailabilityServices createHaServices(
            Configuration configuration, Executor executor) throws Exception {
        // TODO 创建高可用服务
        return HighAvailabilityServicesUtils.createHighAvailabilityServices(
                configuration,
                executor,
                HighAvailabilityServicesUtils.AddressResolution.NO_ADDRESS_RESOLUTION);
    }

    protected HeartbeatServices createHeartbeatServices(Configuration configuration) {
        return HeartbeatServices.fromConfiguration(configuration);
    }

    protected MetricRegistryImpl createMetricRegistry(
            Configuration configuration, PluginManager pluginManager) {
        return new MetricRegistryImpl(
                MetricRegistryConfiguration.fromConfiguration(configuration),
                ReporterSetup.fromConfiguration(configuration, pluginManager));
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        ShutdownHookUtil.removeShutdownHook(shutDownHook, getClass().getSimpleName(), LOG);

        return shutDownAsync(
                        ApplicationStatus.UNKNOWN,
                        ShutdownBehaviour.STOP_PROCESS,
                        "Cluster entrypoint has been closed externally.",
                        false)
                .thenAccept(ignored -> {});
    }

    protected CompletableFuture<Void> stopClusterServices(boolean cleanupHaData) {
        final long shutdownTimeout =
                configuration.getLong(ClusterOptions.CLUSTER_SERVICES_SHUTDOWN_TIMEOUT);

        synchronized (lock) {
            Throwable exception = null;

            final Collection<CompletableFuture<Void>> terminationFutures = new ArrayList<>(3);

            if (blobServer != null) {
                try {
                    blobServer.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (haServices != null) {
                try {
                    if (cleanupHaData) {
                        haServices.closeAndCleanupAllData();
                    } else {
                        haServices.close();
                    }
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (archivedExecutionGraphStore != null) {
                try {
                    archivedExecutionGraphStore.close();
                } catch (Throwable t) {
                    exception = ExceptionUtils.firstOrSuppressed(t, exception);
                }
            }

            if (processMetricGroup != null) {
                processMetricGroup.close();
            }

            if (metricRegistry != null) {
                terminationFutures.add(metricRegistry.shutdown());
            }

            if (ioExecutor != null) {
                terminationFutures.add(
                        ExecutorUtils.nonBlockingShutdown(
                                shutdownTimeout, TimeUnit.MILLISECONDS, ioExecutor));
            }

            if (commonRpcService != null) {
                terminationFutures.add(commonRpcService.stopService());
            }

            try {
                JMXService.stopInstance();
            } catch (Throwable t) {
                exception = ExceptionUtils.firstOrSuppressed(t, exception);
            }

            if (exception != null) {
                terminationFutures.add(FutureUtils.completedExceptionally(exception));
            }

            return FutureUtils.completeAll(terminationFutures);
        }
    }

    @Override
    public void onFatalError(Throwable exception) {
        ClusterEntryPointExceptionUtils.tryEnrichClusterEntryPointError(exception);
        LOG.error("Fatal error occurred in the cluster entrypoint.", exception);

        System.exit(RUNTIME_FAILURE_RETURN_CODE);
    }

    // --------------------------------------------------
    // Internal methods
    // --------------------------------------------------

    private Configuration generateClusterConfiguration(Configuration configuration) {
        final Configuration resultConfiguration =
                new Configuration(Preconditions.checkNotNull(configuration));

        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);
        final File uniqueWebTmpDir = new File(webTmpDir, "flink-web-" + UUID.randomUUID());

        resultConfiguration.setString(WebOptions.TMP_DIR, uniqueWebTmpDir.getAbsolutePath());

        return resultConfiguration;
    }

    private CompletableFuture<ApplicationStatus> shutDownAsync(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics,
            boolean cleanupHaData) {
        if (isShutDown.compareAndSet(false, true)) {
            LOG.info(
                    "Shutting {} down with application status {}. Diagnostics {}.",
                    getClass().getSimpleName(),
                    applicationStatus,
                    diagnostics);

            final CompletableFuture<Void> shutDownApplicationFuture =
                    closeClusterComponent(applicationStatus, shutdownBehaviour, diagnostics);

            final CompletableFuture<Void> serviceShutdownFuture =
                    FutureUtils.composeAfterwards(
                            shutDownApplicationFuture, () -> stopClusterServices(cleanupHaData));

            final CompletableFuture<Void> cleanupDirectoriesFuture =
                    FutureUtils.runAfterwards(serviceShutdownFuture, this::cleanupDirectories);

            cleanupDirectoriesFuture.whenComplete(
                    (Void ignored2, Throwable serviceThrowable) -> {
                        if (serviceThrowable != null) {
                            terminationFuture.completeExceptionally(serviceThrowable);
                        } else {
                            terminationFuture.complete(applicationStatus);
                        }
                    });
        }

        return terminationFuture;
    }

    /**
     * Close cluster components and deregister the Flink application from the resource management
     * system by signalling the {@link ResourceManager}.
     *
     * @param applicationStatus to terminate the application with
     * @param shutdownBehaviour shutdown behaviour
     * @param diagnostics additional information about the shut down, can be {@code null}
     * @return Future which is completed once the shut down
     */
    private CompletableFuture<Void> closeClusterComponent(
            ApplicationStatus applicationStatus,
            ShutdownBehaviour shutdownBehaviour,
            @Nullable String diagnostics) {
        synchronized (lock) {
            if (clusterComponent != null) {
                switch (shutdownBehaviour) {
                    case STOP_APPLICATION:
                        return clusterComponent.stopApplication(applicationStatus, diagnostics);
                    case STOP_PROCESS:
                    default:
                        return clusterComponent.stopProcess();
                }
            } else {
                return CompletableFuture.completedFuture(null);
            }
        }
    }

    /**
     * Clean up of temporary directories created by the {@link ClusterEntrypoint}.
     *
     * @throws IOException if the temporary directories could not be cleaned up
     */
    protected void cleanupDirectories() throws IOException {
        final String webTmpDir = configuration.getString(WebOptions.TMP_DIR);

        FileUtils.deleteDirectory(new File(webTmpDir));
    }

    // --------------------------------------------------
    // Abstract methods
    // --------------------------------------------------

    protected abstract DispatcherResourceManagerComponentFactory
            createDispatcherResourceManagerComponentFactory(Configuration configuration)
                    throws IOException;

    protected abstract ArchivedExecutionGraphStore createSerializableExecutionGraphStore(
            Configuration configuration, ScheduledExecutor scheduledExecutor) throws IOException;

    protected static EntrypointClusterConfiguration parseArguments(String[] args)
            throws FlinkParseException {
        final CommandLineParser<EntrypointClusterConfiguration> clusterConfigurationParser =
                new CommandLineParser<>(new EntrypointClusterConfigurationParserFactory());

        return clusterConfigurationParser.parse(args);
    }

    protected static Configuration loadConfiguration(
            EntrypointClusterConfiguration entrypointClusterConfiguration) {
        final Configuration dynamicProperties =
                ConfigurationUtils.createConfiguration(
                        entrypointClusterConfiguration.getDynamicProperties());
        final Configuration configuration =
                GlobalConfiguration.loadConfiguration(
                        entrypointClusterConfiguration.getConfigDir(), dynamicProperties);

        final int restPort = entrypointClusterConfiguration.getRestPort();

        if (restPort >= 0) {
            configuration.setInteger(RestOptions.PORT, restPort);
        }

        final String hostname = entrypointClusterConfiguration.getHostname();

        if (hostname != null) {
            configuration.setString(JobManagerOptions.ADDRESS, hostname);
        }

        return configuration;
    }

    // --------------------------------------------------
    // Helper methods
    // --------------------------------------------------

    public static void runClusterEntrypoint(ClusterEntrypoint clusterEntrypoint) {

        final String clusterEntrypointName = clusterEntrypoint.getClass().getSimpleName();
        try {
            clusterEntrypoint.startCluster();
        } catch (ClusterEntrypointException e) {
            LOG.error(
                    String.format("Could not start cluster entrypoint %s.", clusterEntrypointName),
                    e);
            System.exit(STARTUP_FAILURE_RETURN_CODE);
        }

        int returnCode;
        Throwable throwable = null;

        try {
            returnCode = clusterEntrypoint.getTerminationFuture().get().processExitCode();
        } catch (Throwable e) {
            throwable = ExceptionUtils.stripExecutionException(e);
            returnCode = RUNTIME_FAILURE_RETURN_CODE;
        }

        LOG.info(
                "Terminating cluster entrypoint process {} with exit code {}.",
                clusterEntrypointName,
                returnCode,
                throwable);
        System.exit(returnCode);
    }

    /** Execution mode of the {@link MiniDispatcher}. */
    public enum ExecutionMode {
        /** Waits until the job result has been served. */
        NORMAL,

        /** Directly stops after the job has finished. */
        DETACHED
    }

    private enum ShutdownBehaviour {
        STOP_APPLICATION,
        STOP_PROCESS,
    }
}
