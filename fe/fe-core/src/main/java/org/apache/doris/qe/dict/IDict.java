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

public abstract class IDict {

	private long dictId;
	private DictState state;
	private long dbId;
	private long tableId;
	private String columnName;
	private long lastAccessTime;
	// Used to check whether the dict is valid
	private long dataVersion = 1;

	public IDict(long dictId, long dbId, long tableId, String columnName) {
		this.dictId = dictId;
		this.state = DictState.INVALID;
		this.dbId = dbId;
		this.tableId = tableId;
		this.columnName = columnName;
		this.lastAccessTime = System.currentTimeMillis() / 1000;
	}
	
	
	// Init the dict from mysql buffer
	public abstract void initDict();
	
	// Merge current dict with the other one
	public abstract void mergeWith(IDict dict);
	
	// Has to lock the dictsMap to prevent invalid version change
	public IDict refresh() {
		long startUpdateVersion = dataVersion;
		IDict newDict = doRefresh();
		newDict.dataVersion = startUpdateVersion;
		newDict.state = DictState.VALID;
		return newDict;
	}
	
	public boolean dataChanged(IDict newDict) {
		return dataVersion != newDict.dataVersion;
	}
	
	public void copyDictState(IDict oldDict) {
		this.dataVersion = oldDict.dataVersion;
	}
	
	// Update the dict 
	//     for string dict, call select dict(str_col_name) from tablename [meta];
	//	   for int dict, ...
	// Every Dict is immutable, if refresh is called, then a new Dict is generated
	public abstract IDict doRefresh();
	
	// Return thrift definition of this dict
	public abstract void toThrift();
	
	public void invalidDict() {
		this.state = DictState.INVALID;
		++ this.dataVersion;
	}
	
	public DictState getState() { return state; }
	
	public void updateLastAccessTime() { this.lastAccessTime = System.currentTimeMillis() / 1000; }
	
	public long getLastAccessTime() { return lastAccessTime; }
	
	public DictKey getDictKey() { return new DictKey(tableId, columnName); }

	public long getDictId() { return dictId; }
	
	public void resetDictId(long newId) { this.dictId = newId; }


	public long getDbId() {
		return dbId;
	}


	public long getTableId() {
		return tableId;
	}


	public String getColumnName() {
		return columnName;
	}
}
