/* Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.camunda.tngp.broker.workflow;

import static org.camunda.tngp.broker.logstreams.LogStreamServiceNames.SNAPSHOT_STORAGE_SERVICE;
import static org.camunda.tngp.broker.logstreams.LogStreamServiceNames.logStreamServiceName;
import static org.camunda.tngp.broker.logstreams.processor.StreamProcessorIds.DEPLOYMENT_PROCESSOR_ID;
import static org.camunda.tngp.broker.logstreams.processor.StreamProcessorIds.WORKFLOW_PROCESSOR_ID;
import static org.camunda.tngp.broker.system.SystemServiceNames.AGENT_RUNNER_SERVICE;
import static org.camunda.tngp.broker.workflow.WorkflowQueueServiceNames.deploymentStreamProcessorServiceName;
import static org.camunda.tngp.broker.workflow.WorkflowQueueServiceNames.workflowStreamProcessorServiceName;

import java.io.File;
import java.nio.channels.FileChannel;
import java.util.List;

import org.agrona.concurrent.Agent;
import org.camunda.tngp.broker.logstreams.processor.StreamProcessorService;
import org.camunda.tngp.broker.system.ConfigurationManager;
import org.camunda.tngp.broker.system.threads.AgentRunnerServices;
import org.camunda.tngp.broker.transport.clientapi.CommandResponseWriter;
import org.camunda.tngp.broker.workflow.cfg.WorkflowQueueCfg;
import org.camunda.tngp.broker.workflow.processor.DeploymentStreamProcessor;
import org.camunda.tngp.broker.workflow.processor.WorkflowInstanceStreamProcessor;
import org.camunda.tngp.dispatcher.Dispatcher;
import org.camunda.tngp.hashindex.store.FileChannelIndexStore;
import org.camunda.tngp.hashindex.store.IndexStore;
import org.camunda.tngp.logstreams.log.LogStream;
import org.camunda.tngp.logstreams.processor.StreamProcessorController;
import org.camunda.tngp.servicecontainer.Injector;
import org.camunda.tngp.servicecontainer.Service;
import org.camunda.tngp.servicecontainer.ServiceGroupReference;
import org.camunda.tngp.servicecontainer.ServiceName;
import org.camunda.tngp.servicecontainer.ServiceStartContext;
import org.camunda.tngp.servicecontainer.ServiceStopContext;
import org.camunda.tngp.util.DeferredCommandContext;
import org.camunda.tngp.util.FileUtil;

public class WorkflowQueueManagerService implements Service<WorkflowQueueManager>, WorkflowQueueManager, Agent
{
    protected static final String NAME = "workflow.queue.manager";

    protected final Injector<Dispatcher> sendBufferInjector = new Injector<>();
    protected final Injector<AgentRunnerServices> agentRunnerServicesInjector = new Injector<>();

    protected final ServiceGroupReference<LogStream> logStreamsGroupReference = ServiceGroupReference.<LogStream>create()
            .onAdd((name, stream) -> addStream(stream, name))
            .build();

    protected final List<WorkflowQueueCfg> queueConfigs;

    protected ServiceStartContext serviceContext;
    protected DeferredCommandContext asyncContext;

    public WorkflowQueueManagerService(ConfigurationManager configurationManager)
    {
        this.queueConfigs = configurationManager.readList("workflow-queue", WorkflowQueueCfg.class);
    }

    @Override
    public void startWorkflowQueue(WorkflowQueueCfg config)
    {
        final String logName = config.logName;
        if (logName == null || logName.isEmpty())
        {
            throw new RuntimeException("Cannot start workflow queue: Mandatory configuration property 'logName' is not set.");
        }

        installDeploymentStreamProcessor(config, logName);

        installWorkflowStreamProcessor(config, logName);
    }

    private void installDeploymentStreamProcessor(WorkflowQueueCfg config, String logName)
    {
        final ServiceName<StreamProcessorController> streamProcessorServiceName = deploymentStreamProcessorServiceName(logName);
        final String streamProcessorName = streamProcessorServiceName.getName();

        final IndexStore indexStore = createIndexStore(config, "workflow.deployment");

        final Dispatcher sendBuffer = sendBufferInjector.getValue();
        final CommandResponseWriter responseWriter = new CommandResponseWriter(sendBuffer);
        final ServiceName<LogStream> logStreamServiceName = logStreamServiceName(logName);

        final DeploymentStreamProcessor deploymentStreamProcessor = new DeploymentStreamProcessor(responseWriter, indexStore);
        final StreamProcessorService deployemtStreamProcessorService = new StreamProcessorService(
                streamProcessorName,
                DEPLOYMENT_PROCESSOR_ID,
                deploymentStreamProcessor)
                .eventFilter(DeploymentStreamProcessor.eventFilter());

        serviceContext.createService(streamProcessorServiceName, deployemtStreamProcessorService)
                .dependency(logStreamServiceName, deployemtStreamProcessorService.getSourceStreamInjector())
                .dependency(logStreamServiceName, deployemtStreamProcessorService.getTargetStreamInjector())
                .dependency(SNAPSHOT_STORAGE_SERVICE, deployemtStreamProcessorService.getSnapshotStorageInjector())
                .dependency(AGENT_RUNNER_SERVICE, deployemtStreamProcessorService.getAgentRunnerInjector())
                .install();
    }

    private void installWorkflowStreamProcessor(WorkflowQueueCfg config, String logName)
    {
        final ServiceName<StreamProcessorController> streamProcessorServiceName = workflowStreamProcessorServiceName(logName);
        final String streamProcessorName = streamProcessorServiceName.getName();

        final IndexStore workflowPositionIndexStore = createIndexStore(config, "workflow.instance.position");
        final IndexStore workflowVersionIndexStore = createIndexStore(config, "workflow.instance.version");

        final Dispatcher sendBuffer = sendBufferInjector.getValue();
        final CommandResponseWriter responseWriter = new CommandResponseWriter(sendBuffer);
        final ServiceName<LogStream> logStreamServiceName = logStreamServiceName(logName);

        final WorkflowInstanceStreamProcessor workflowInstanceStreamProcessor = new WorkflowInstanceStreamProcessor(responseWriter, workflowPositionIndexStore, workflowVersionIndexStore);
        final StreamProcessorService workflowStreamProcessorService = new StreamProcessorService(
                streamProcessorName,
                WORKFLOW_PROCESSOR_ID,
                workflowInstanceStreamProcessor)
                .eventFilter(WorkflowInstanceStreamProcessor.eventFilter());

        serviceContext.createService(streamProcessorServiceName, workflowStreamProcessorService)
                .dependency(logStreamServiceName, workflowStreamProcessorService.getSourceStreamInjector())
                .dependency(logStreamServiceName, workflowStreamProcessorService.getTargetStreamInjector())
                .dependency(SNAPSHOT_STORAGE_SERVICE, workflowStreamProcessorService.getSnapshotStorageInjector())
                .dependency(AGENT_RUNNER_SERVICE, workflowStreamProcessorService.getAgentRunnerInjector())
                .install();
    }

    private IndexStore createIndexStore(WorkflowQueueCfg config, String indexName)
    {
        final IndexStore indexStore;

        if (config.useTempIndexFile)
        {
            indexStore = FileChannelIndexStore.tempFileIndexStore();
        }
        else if (config.indexDirectory != null && !config.indexDirectory.isEmpty())
        {
            final String indexFile = config.indexDirectory + File.separator + indexName + ".idx";
            final FileChannel indexFileChannel = FileUtil.openChannel(indexFile, true);
            indexStore = new FileChannelIndexStore(indexFileChannel);
        }
        else
        {
            throw new RuntimeException(String.format("Cannot create stream processor index, no index file name provided."));
        }
        return indexStore;
    }

    @Override
    public void start(ServiceStartContext serviceContext)
    {
        this.serviceContext = serviceContext;
        this.asyncContext = new DeferredCommandContext();

        final AgentRunnerServices agentRunnerService = agentRunnerServicesInjector.getValue();
        agentRunnerService.conductorAgentRunnerService().run(this);
    }

    @Override
    public void stop(ServiceStopContext ctx)
    {
        ctx.run(() ->
        {
            final AgentRunnerServices agentRunnerService = agentRunnerServicesInjector.getValue();
            agentRunnerService.conductorAgentRunnerService().remove(this);
        });
    }

    @Override
    public WorkflowQueueManager get()
    {
        return this;
    }

    public Injector<Dispatcher> getSendBufferInjector()
    {
        return sendBufferInjector;
    }

    public ServiceGroupReference<LogStream> getLogStreamsGroupReference()
    {
        return logStreamsGroupReference;
    }

    public Injector<AgentRunnerServices> getAgentRunnerServicesInjector()
    {
        return agentRunnerServicesInjector;
    }

    public void addStream(LogStream logStream, ServiceName<LogStream> logStreamServiceName)
    {
        asyncContext.runAsync((r) ->
        {
            for (int i = 0; i < queueConfigs.size(); i++)
            {
                final String logName = logStream.getLogName();
                final WorkflowQueueCfg config = queueConfigs.get(i);
                if (logName.equals(config.logName))
                {
                    startWorkflowQueue(config);
                    break;
                }
            }
        });
    }

    @Override
    public int doWork() throws Exception
    {
        return asyncContext.doWork();
    }

    @Override
    public String roleName()
    {
        return NAME;
    }

}
