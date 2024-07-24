// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.resource;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.Pair;
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.resource.workloadgroup.QueueToken;
import org.apache.doris.resource.workloadgroup.WorkloadGroup;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TGetBeResourceRequest;
import org.apache.doris.thrift.TGetBeResourceResult;
import org.apache.doris.thrift.TGlobalResourceUsage;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class AdmissionControl extends MasterDaemon {

    public static final Logger LOG = LogManager.getLogger(AdmissionControl.class);

    private volatile boolean isAllBeMemoryEnough = true;

    private double currentMemoryLimit = 0;

    private SystemInfoService clusterInfoService;

    private ConcurrentLinkedQueue<QueueToken> queryWaitQueue = new ConcurrentLinkedQueue();

    private List<GetBeResourceUsageResponse> backendResourceUsageStats = Lists.newArrayList();

    private final ExecutorService fetchBackendStatsPool;

    public AdmissionControl(SystemInfoService clusterInfoService) {
        super("get-be-resource-usage-thread", Config.get_be_resource_usage_interval_ms);
        this.clusterInfoService = clusterInfoService;
        fetchBackendStatsPool = ThreadPoolManager.newDaemonFixedThreadPool(
                50, 100, "fetch-backend-stats-pool", true);
    }

    public void addQueueToken(QueueToken queryQueue) {
        queryWaitQueue.offer(queryQueue);
    }

    @Override
    protected void runAfterCatalogReady() {

        if (Config.query_queue_by_be_used_memory > 0) {
            this.backendResourceUsageStats = collectBackendResourceUsage(clusterInfoService.getIdToBackend().values());
            this.isAllBeMemoryEnough = checkBackendResourceUsage(this.backendResourceUsageStats);
        } else {
            this.isAllBeMemoryEnough = true;
        }
        notifyWaitQuery();
    }

    private boolean checkBackendResourceUsage(List<GetBeResourceUsageResponse> backendResourceUsageStats) {
        boolean tmpIsAllBeMemoryEnough = true;
        List<GetBeResourceUsageResponse> collectBackendResourceUsage = collectBackendResourceUsage(backends);
        for (GetBeResourceUsageResponse backendResponse : collectBackendResourceUsage) {
            TGetBeResourceResult result = backendResponse.tResult;
            if (result != null && result.isSetGlobalResourceUsage()) {
                TGlobalResourceUsage globalResourceUsage = result.getGlobalResourceUsage();
                if (globalResourceUsage != null && globalResourceUsage.isSetMemLimit()
                        && globalResourceUsage.isSetMemUsage()) {
                    long memUsageL = globalResourceUsage.getMemUsage();
                    long memLimitL = globalResourceUsage.getMemLimit();
                    double memUsage = Double.valueOf(String.valueOf(memUsageL));
                    double memLimit = Double.valueOf(String.valueOf(memLimitL));
                    double memUsagePercent = memUsage / memLimit;

                    if (memUsagePercent > Config.query_queue_by_be_used_memory) {
                        return false;
                    }
                    LOG.debug(
                            "be ip:{}, mem limit:{}, mem usage:{}, mem usage percent:{}, "
                                    + "query queue mem:{}, query wait size:{}",
                                    backendResponse.backend.getHost(), memLimitL,
                                    memUsageL, memUsagePercent, this.currentMemoryLimit,
                            this.queryWaitQueue.size());
                }
            }
        }
    }

    // Check if the workload group on specified backends meets requirement
    public boolean checkWorkloadGroupResourceUsage(Collection<Backend> backends, WorkloadGroup workloadGroup,
            long timeoutMillis) {
        List<GetBeResourceUsageResponse> backendStats = collectBackendResourceUsage(backends);
        boolean allBackendResourceAvailable = checkBackendResourceUsage(backendStats);
        if (!allBackendResourceAvailable) {
            return false;
        }
        return true;
    }

    // Send concurrent request to all backend async to reduce latency
    private List<GetBeResourceUsageResponse> collectBackendResourceUsage(Collection<Backend> backends) {
        List<Future<GetBeResourceUsageResponse>> futures = Lists.newArrayList();
        for (Backend be: backends) {
            if (!be.isAlive()) {
                continue;
            }
            Callable<GetBeResourceUsageResponse> task = () -> {
                TNetworkAddress address = null;
                BackendService.Client client = null;
                TGetBeResourceResult result = null;
                boolean rpcOk = true;
                try {
                    address = new TNetworkAddress(be.getHost(), be.getBePort());
                    client = ClientPool.backendPool.borrowObject(address, 5000);
                    result = client.getBeResource(new TGetBeResourceRequest());
                } catch (Throwable t) {
                    rpcOk = false;
                    LOG.warn("get be {} resource failed, ", be.getHost(), t);
                } finally {
                    try {
                        if (rpcOk) {
                            ClientPool.backendPool.returnObject(address, client);
                        } else {
                            ClientPool.backendPool.invalidateObject(address, client);
                        }
                    } catch (Throwable e) {
                        LOG.warn("return rpc client failed. related backend[{}]", be.getHost(),
                                e);
                    }
                }
                return new GetBeResourceUsageResponse(be, result);
            };
            Future<GetBeResourceUsageResponse> future = fetchBackendStatsPool.submit(task);
            futures.add(future);
        }
        List<GetBeResourceUsageResponse> res = Lists.newArrayList();
        for(Future<GetBeResourceUsageResponse> future : futures) {
            GetBeResourceUsageResponse beResource = future.get(1, TimeUnit.SECONDS);
        }
        return res;
    }

    public void notifyWaitQuery() {
        if (!isAllBeMemoryEnough()) {
            return;
        }
        int waitQueryCountSnapshot = queryWaitQueue.size();
        Iterator<QueueToken> queueTokenIterator = queryWaitQueue.iterator();
        while (waitQueryCountSnapshot > 0 && queueTokenIterator.hasNext()) {
            QueueToken queueToken = queueTokenIterator.next();
            queueToken.notifyWaitQuery();
            waitQueryCountSnapshot--;
        }
    }

    public void removeQueueToken(QueueToken queueToken) {
        queryWaitQueue.remove(queueToken);
    }

    public boolean isAllBeMemoryEnough() {
        return isAllBeMemoryEnough;
    }

    //TODO(wb): add more resource type
    public boolean checkResourceAvailable(QueueToken queueToken) {
        if (isAllBeMemoryEnough()) {
            return true;
        } else {
            queueToken.setQueueMsg("WAIT_BE_MEMORY");
            return false;
        }
    }

    class GetBeResourceUsageResponse {
        final Backend backend;
        final TGetBeResourceResult tResult;
        public GetBeResourceUsageResponse(Backend backend, TGetBeResourceResult tResult) {
            this.backend = backend;
            this.tResult = tResult;
        }
    }

}
