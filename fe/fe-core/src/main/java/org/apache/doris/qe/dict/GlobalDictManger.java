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

package org.apache.doris.qe.dict;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.Daemon;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InternalQueryExecutor;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TExtRowBatch;
import org.apache.doris.thrift.TResultBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.alibaba.google.common.collect.Lists;
import com.alibaba.google.common.collect.Maps;

public class GlobalDictManger extends Daemon {
    private static final Logger LOG = LogManager.getLogger(GlobalDictManger.class);
	private Map<DictKey, IDict> dictsMap = null;
	
	public GlobalDictManger() {
		super("GLOBAL_DICT_MGR", Config.dict_check_interval_sec * 1000L);
		this.dictsMap = Maps.newConcurrentMap();
	}

	// During query, if dict is invalid, then optimizer should not use it any more
	public IDict getDictForQuery(long tableId, String column) {
		IDict dict = this.dictsMap.get(new DictKey(tableId, column));
		if (dict.getState() == DictState.INVALID) {
			return null;
		}
		dict.updateLastAccessTime();
		return dict;
	}

	// During load, if dict is invalid, has to return it, and the load process will
	// check if the dict is up to date
	public IDict getDictForLoad(long tableId, String column) {
		IDict dict = this.dictsMap.get(new DictKey(tableId, column));
		dict.updateLastAccessTime();
		return dict;
	}
	
	public void invalidDict(long tableId, String column) {
		IDict dict = dictsMap.get(new DictKey(tableId, column));
		if (dict != null) {
			dict.invalidDict();
		}
	}
	
	// The optimizer could call this method to set the specific <tableid, columnname> 
	// to generate dict for it and it could be used in the future for query accelerating
	private void addDict(DictKey dictKey, IDict dict) {
		dict.invalidDict();
		dictsMap.put(dictKey, dict);
	}
	
	@Override
	protected void runOneCycle() {
		if (true) {
			ConnectContext connectContext = new ConnectContext();
			String stmt = "select distinct lo_shipmode from lineorder;";
			InternalQueryExecutor queryExecutor = new InternalQueryExecutor(connectContext, stmt);
			try {
				queryExecutor.execute();
				TResultBatch resultBatch = queryExecutor.getNext();
				TExtRowBatch rowBatch = resultBatch.getThriftRowBatch();
			} catch (Exception e) {
				LOG.info("errors while execute query ", e);
			}
			return;
		}
		// Traverse all dict, if the dict is not accessed for a long period of time, then delete it
		long curTime = System.currentTimeMillis() / 1000;
		List<DictKey> dictToRemove = Lists.newArrayList();
		for (IDict dict : dictsMap.values()) {
			if (dict.getLastAccessTime() - curTime > Config.dict_expire_sec) {
				dictToRemove.add(dict.getDictKey());
			}
		}
		for (DictKey key : dictToRemove) {
			dictsMap.remove(key);
		}
		
		// Traverse all columns from catalog and check if it is needed to generate global dict
		// This is a temporary work, because doris does not have a wonderful optimizer, so that
		// check all column here instead.
		// Maybe this method could be moved to catalog class
		Catalog catalog = Catalog.getCurrentCatalog();
		List<Long> dbIds = catalog.getDbIds();
		for (Long dbId : dbIds) {
			Database db = catalog.getDbNullable(dbId);
			if (db == null) {
				continue;
			}
			List<Table> tables = db.getTables();
			for (Table table : tables) {
				if (table instanceof OlapTable) {
					OlapTable olapTable = (OlapTable) table;
					List<Column> allColumns = olapTable.getFullSchema();	
					// TODO check if the column is low cardinality
					for (Column column : allColumns) {
						// if it is low cardinality and it string
						if (column.getType() == Type.CHAR 
								|| column.getType() == Type.VARCHAR
								|| column.getType() == Type.STRING) {
							addDict(new DictKey(olapTable.getId(), column.getName()), new StringDict(catalog.getNextId(), db.getId() ,olapTable.getId(), column.getName()));
						}
					}
				}
			}
		}
        // Call select dict(col) from table[meta] to get dict
		List<IDict> dictToRefresh = Lists.newArrayList();
		for (IDict dict : dictsMap.values()) {
			if (dict.getState() != DictState.VALID) {
				dictToRefresh.add(dict);
			}
		}
		for (IDict dict : dictToRefresh) {
			IDict newDict = dict.refresh();
			// TODO lock here, ensure dict not changed
			if (newDict.dataChanged(dict)) {
				newDict.copyDictState(dict);
				newDict.invalidDict();
			}
			// If cur data version is 10, then use version = 10 to refresh dict, but current load is using old
			// dict, then it will increase data version to a larger value for example 15. After refresh finished, then
			// the dict will find that version 10 < version 15, then it will invalid the dict. If we do not replace the 
			// old dict, load process will increase data version to more larger value and we could not catchup. But if we 
			// replace the old dict with dict (version = 10), then the new key maybe covered by the new dict, load process
			// will use the new dict to encode data, it will find there is no new key. Then the data version will kept to be
			// 15. Then in next round dict will refresh to 15. And it is valid, could be used during query.
			// replace the old dict with new dict, then load process will use the newly dict
			// if the load process does not find any new key, then the it will not call invalid
			newDict.resetDictId(catalog.getNextId());
			dictsMap.put(dict.getDictKey(), newDict);
		}
    }
}
