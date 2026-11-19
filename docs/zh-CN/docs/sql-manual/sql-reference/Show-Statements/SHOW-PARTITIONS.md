---
{
    "title": "SHOW-PARTITIONS",
    "language": "zh-CN"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

## SHOW-PARTITIONS

### Name

SHOW PARTITIONS

### Description

该语句用于展示分区信息。支持 Internal catalog 和 Hive Catalog

语法：

```SQL
 SHOW [TEMPORARY] PARTITIONS FROM [db_name.]table_name [WHERE] [ORDER BY] [LIMIT];
```

说明:

对于 Internal catalog：
1. 支持PartitionId,PartitionName,State,Buckets,ReplicationNum,LastConsistencyCheckTime等列的过滤
2. TEMPORARY指定列出临时分区

对于内部表，结果包含以下分区级别的版本信息：

- `VisibleVersion`：正常查询使用的可见版本。
- `NextVersion`：下一次事务提交时将分配的版本。
- `CommittedVersion`：最新已提交版本，等于 `NextVersion - 1`。存在等待发布的事务时，它可能大于 `VisibleVersion`。

`NextVersion` 和 `CommittedVersion` 追加在原有列之后。`SHOW PROC '/dbs/<db_id>/<table_id>/partitions'` 及对应的 `temp_partitions` 路径也展示这两列。这些命令只展示元数据，不修改版本。

<version since="dev">

对于 Hive Catalog：
支持返回所有分区，包括多级分区

</version>

### Example

1. 展示指定db下指定表的所有非临时分区信息

    ```SQL
    SHOW PARTITIONS FROM example_db.table_name;
    ```

2. 展示指定db下指定表的所有临时分区信

    ```SQL
    SHOW TEMPORARY PARTITIONS FROM example_db.table_name;
    ```

3. 展示指定db下指定表的指定非临时分区的信息

    ```SQL
     SHOW PARTITIONS FROM example_db.table_name WHERE PartitionName = "p1";
    ```

4. 展示指定db下指定表的最新非临时分区的信息

    ```SQL
    SHOW PARTITIONS FROM example_db.table_name ORDER BY PartitionId DESC LIMIT 1;
    ```

### Keywords

    SHOW, PARTITIONS

### Best Practice

