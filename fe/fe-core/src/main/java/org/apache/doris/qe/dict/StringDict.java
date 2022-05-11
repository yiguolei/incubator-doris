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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.InternalQueryExecutor;

public class StringDict extends IDict {

	public StringDict(long dictId, long dbId, long tableId, String columnName) {
		super(dictId, dbId, tableId, columnName);
	}
	
	@Override
	public void initDict() {
		
	}

	@Override
	public void mergeWith(IDict dict) {
		// do nothing, not implement it yet
	}

	@Override
	public IDict doRefresh() {
		Catalog catalog = Catalog.getCurrentCatalog();
		Database db = catalog.getDbNullable(getDbId());
		Table table = db.getTableNullable(getTableId());
		ConnectContext connectContext = new ConnectContext();
		String stmt = "select distinct " + getColumnName() + " from " + db.getFullName() + "." + table.getName() + "[meta]";
		InternalQueryExecutor queryExecutor = new InternalQueryExecutor(connectContext, stmt);
		return null;
	}

	@Override
	public void toThrift() {
		
	}

}
