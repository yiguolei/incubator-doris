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

package org.apache.doris.common.proc;

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.DataProperty;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.KeysType;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.RandomDistributionInfo;
import org.apache.doris.catalog.ReplicaAllocation;
import org.apache.doris.catalog.SinglePartitionInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.thrift.TStorageMedium;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class PartitionsProcDirTest {
    @Test
    public void testCommittedVersionAheadOfVisibleVersion() throws AnalysisException {
        checkVersions(false, 10L, 13L, "12");
    }

    @Test
    public void testTemporaryPartitionVersions() throws AnalysisException {
        checkVersions(true, 2147483648L, 2147483651L, "2147483650");
    }

    @Test
    public void testInitialPartitionVersions() throws AnalysisException {
        checkVersions(false, 1L, 2L, "1");
    }

    private void checkVersions(boolean temporary, long visibleVersion, long nextVersion,
            String committedVersion) throws AnalysisException {
        Database db = new Database(1L, "test_db");
        PartitionInfo partitionInfo = new SinglePartitionInfo();
        RandomDistributionInfo distributionInfo = new RandomDistributionInfo(1);
        OlapTable table = new OlapTable(2L, "test_table", Collections.emptyList(), KeysType.DUP_KEYS,
                partitionInfo, distributionInfo);
        Partition partition = new Partition(3L, "p1",
                new MaterializedIndex(4L, MaterializedIndex.IndexState.NORMAL), distributionInfo);
        partition.updateVisibleVersion(visibleVersion);
        partition.setNextVersion(nextVersion);
        partitionInfo.addPartition(3L, new DataProperty(TStorageMedium.HDD),
                ReplicaAllocation.DEFAULT_ALLOCATION, false, true);
        if (temporary) {
            table.addTempPartition(partition);
        } else {
            table.addPartition(partition);
        }

        PartitionsProcDir proc = new PartitionsProcDir(db, table, temporary);
        ProcResult result = proc.fetchResult();
        List<String> expectedColumns = Arrays.asList("PartitionId", "PartitionName", "VisibleVersion",
                "VisibleVersionTime", "State", "PartitionKey", "Range", "DistributionKey", "Buckets",
                "ReplicationNum", "StorageMedium", "CooldownTime", "RemoteStoragePolicy",
                "LastConsistencyCheckTime", "DataSize", "IsInMemory", "ReplicaAllocation", "IsMutable",
                "NextVersion", "CommittedVersion");
        Assert.assertEquals(expectedColumns, result.getColumnNames());
        Assert.assertEquals(1, result.getRows().size());
        List<String> row = result.getRows().get(0);
        Assert.assertEquals(expectedColumns.size(), row.size());
        Assert.assertEquals(Long.toString(visibleVersion), row.get(2));
        Assert.assertEquals(Long.toString(nextVersion), row.get(18));
        Assert.assertEquals(committedVersion, row.get(19));

        // SHOW PARTITIONS uses this path, including its column-count check when filtering.
        ProcResult filtered = proc.fetchResultByFilter(Collections.singletonMap("partitionname",
                new BinaryPredicate(BinaryPredicate.Operator.EQ, new SlotRef(null, "PartitionName"),
                        new StringLiteral("p1"))), null, null);
        Assert.assertEquals(expectedColumns, filtered.getColumnNames());
        Assert.assertEquals(result.getRows(), filtered.getRows());
    }
}
