/*
   Copyright 2007-2013 Hendrik Iben, University Bremen

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
package org.tzi.context.server;

import org.tzi.context.abstractenv.AbstractItem;
import org.tzi.context.abstractenv.ContextAbstraction;
import org.tzi.context.abstractenv.Environment;
import org.tzi.context.abstractenv.PropertyAbstraction;
import org.tzi.context.abstractenv.SourceAbstraction;

public class HistoryDBEnvironment extends Environment {
	public ContextAbstraction createDBContext(int id, String name) {
		if(name==null)
			throw new RuntimeException("name may not be null!");
		
		ContextAbstraction c = getContextByName(name);
		if(c != null)
			return c;
		
		if(id < 0)
			throw new RuntimeException("Invalid db id: " + id);

		ContextAbstraction ctx = addContext(new ContextAbstraction(this, id, name));
		return ctx;
	}
	
	public SourceAbstraction createDBSource(ContextAbstraction ctx, int id, String name) {
		if(name==null)
			throw new RuntimeException("name may not be null!");
		
		SourceAbstraction s = ctx.getSourceByName(name);
		
		if(s!=null)
			return s;

		if(id < 0)
			throw new RuntimeException("Invalid db id: " + id);

		return new SourceAbstraction(ctx, id, name);
	}
	
	public PropertyAbstraction createDBProperty(SourceAbstraction src, int id, String name) {
		if(name==null)
			throw new RuntimeException("name may not be null!");

		PropertyAbstraction p = src.getPropertyByName(name);
		
		if(p!=null)
			return p;
	
		if(id < 0)
			throw new RuntimeException("Invalid db id: " + id);

		return new PropertyAbstraction(src, id, name);
	}
	
	public AbstractItem getKnownItembyId(int id) {
		throw new RuntimeException("Not supported!");
	}
	
	public void removeKnownItem(AbstractItem ai) {
	}
	
	public <A extends AbstractItem> A addKnownItem(A ai) {
		return ai;
	}
}
