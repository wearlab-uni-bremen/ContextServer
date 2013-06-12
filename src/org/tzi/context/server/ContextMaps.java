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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.tzi.context.abstractenv.ContextAbstraction;
import org.tzi.context.abstractenv.Environment;
import org.tzi.context.abstractenv.PropertyAbstraction;
import org.tzi.context.abstractenv.SourceAbstraction;
import org.tzi.context.common.Context;


public class ContextMaps {
	public Map<String, List<ClientProxyListener>> pendingContextListener = new TreeMap<String, List<ClientProxyListener>>();

	private Object ctxKey = new Object();
	
	private Environment env = new Environment();

	public Map<Integer, Map<Integer, ClientProxyListener>> proxyListenerMap = new TreeMap<Integer, Map<Integer, ClientProxyListener>>();
	
	public Context getContextById(int id) {
		ContextAbstraction ca = env.getContextById(id);
		
		return ca == null ? null : (Context)ca.getData(ctxKey);
	}
	
	public ContextMaps() {
		ContextAbstraction ctx = env.createSpecialContext(-1, Context.ALL_CONTEXTS);
		SourceAbstraction src = env.createSpecialSource(ctx, -1, Context.ALL_SOURCES);
		env.createSpecialProperty(src, -1, Context.ALL_PROPERTIES);
	}
	
	public Integer getContextIdByName(String name) {
		ContextAbstraction ca = env.getContextByName(name);
		return ca == null ? null : ca.getId();
	}
	
	public Context getContextByName(String name) {
		Integer id = getContextIdByName(name);
		return id == null ? null : getContextById(id);
	}
	
	public int addContext(Context ctx) {
		if(ctx == null)
			throw new RuntimeException("null-context not allowed!");
		
		ContextAbstraction ca = env.createContext(ctx.getName());
		ca.setData(ctxKey, ctx);
		
		return ca.getId();
	}
	
	public void removeContext(int id) {
		Context ctx = getContextById(id);
		if(ctx==null)
			return;
		env.removeContext(env.getContextById(id));
	}
	
	public Map<Integer, Context> getContextMap() {
		Map<Integer, Context> ctxMap = new TreeMap<Integer, Context>();
		for(ContextAbstraction ca : env.getContexts()) {
			ctxMap.put(ca.getId(), (Context)ca.getData(ctxKey));
		}
		return ctxMap;
	}
	
	// src
	
	public Integer getContextIdForSourceId(int id) {
		SourceAbstraction sa = env.getSourceById(id);
		
		return sa == null ? null : sa.getContext().getId();
	}
	
	// prp
	
	public Integer getSourceIdForPropertyId(int id) {
		PropertyAbstraction pa = env.getPropertyById(id);
		
		return pa == null ? null : pa.getSource().getId();
	}
	
	// src name maps
	
	public String getSourceNameById(int srcId) {
		SourceAbstraction sa = env.getSourceById(srcId);
		
		return sa == null ? null : sa.getName();
	}
	
	public Integer getSourceIdByName(int ctxId, String name) {
		ContextAbstraction ca = env.getContextById(ctxId);
		
		if(ca == null)
			return null;
		
		SourceAbstraction sa = ca.getSourceByName(name);
		
		return sa == null ? null : sa.getId();
	}
	
	public Map<Integer, String> getSourceIdNameMapForContextId(int ctxId) {
		ContextAbstraction ca = env.getContextById(ctxId);
		
		if(ca == null)
			return null;

		
		Map<Integer, String> srcMap = new TreeMap<Integer, String>();
		
		for(SourceAbstraction sa : ca.getSources()) {
			srcMap.put(sa.getId(), sa.getName());
		}
		
		return srcMap;
	}

	public Map<String, Integer> getSourceNameIdMapForContextId(int ctxId) {
		ContextAbstraction ca = env.getContextById(ctxId);
		
		if(ca == null)
			return null;

		
		Map<String, Integer> srcMap = new TreeMap<String, Integer>();
		
		for(SourceAbstraction sa : ca.getSources()) {
			srcMap.put(sa.getName(), sa.getId());
		}
		
		return srcMap;
	}
	
	public int addSourceNameIdForContextId(int ctxId, String srcName) {
		ContextAbstraction ca = env.getContextById(ctxId);
		
		if(ca == null)
			throw new RuntimeException("Invalid ContextID!");
		
		SourceAbstraction sa = ca.getSourceByName(srcName);
		
		if(sa!=null)
			return sa.getId();
		
		return env.createSource(ca, srcName).getId();
	}
	
	public void removeSource(int srcId) {
		SourceAbstraction sa = env.getSourceById(srcId);
		
		if(sa==null)
			return;
		
		ContextAbstraction ca = sa.getContext();
		
		ca.removeSource(sa);
	}
	
	// prp
	
	public String getPropertyNameById(int id) {
		PropertyAbstraction pa = env.getPropertyById(id);
		
		return pa == null ? null : pa.getName();
	}
	
	public Integer getPropertyIdByName(int srcId, String name) {
		
		SourceAbstraction sa = env.getSourceById(srcId);
		
		if(sa == null)
			return null;
		
		PropertyAbstraction pa = sa.getPropertyByName(name);
		
		return pa == null ? null : pa.getId();
	}
	
	public Map<Integer, String> getPropertyIdNameMapForSourceId(int srcId) {
		SourceAbstraction sa = env.getSourceById(srcId);
		
		if(sa == null)
			return null;
		
		Map<Integer, String> prpMap = new TreeMap<Integer, String>();
		
		for(PropertyAbstraction pa : sa.getProperties()) {
			prpMap.put(pa.getId(), pa.getName());
		}
		
		return prpMap;
	}

	public Map<String, Integer> getPropertyNameIdMapForSourceId(int srcId) {
		SourceAbstraction sa = env.getSourceById(srcId);
		
		if(sa == null)
			return null;
		
		Map<String, Integer> prpMap = new TreeMap<String, Integer>();
		
		for(PropertyAbstraction pa : sa.getProperties()) {
			prpMap.put(pa.getName(), pa.getId());
		}
		
		return prpMap;
	}
	
	public int addPropertyNameIdForSourceId(int srcId, String prpName) {
		
		SourceAbstraction sa = env.getSourceById(srcId);
		
		if(sa == null)
			throw new RuntimeException("Invalid SourceID!");
		
		PropertyAbstraction pa = sa.getPropertyByName(prpName);
		
		if(pa!=null)
			return pa.getId();
		
		return env.createProperty(sa, prpName).getId();
	}
	
	public void removeProperty(int prpId) {
		
		PropertyAbstraction pa = env.getPropertyById(prpId);
		
		SourceAbstraction sa = pa.getSource();
		
		sa.removeProperty(pa);
	}
	
	public Integer getContextIdForPropertyId(int prpId) {
		Integer srcId = getSourceIdForPropertyId(prpId);
		
		if(srcId==null)
			return null;
		
		return getContextIdForSourceId(srcId);
	}
	
	public Context getContextForSourceId(int srcId) {
		Integer ctxId = getContextIdForSourceId(srcId);
		
		if(ctxId==null)
			return null;
		
		return getContextById(ctxId);
	}
	
	public String getSourceNameForPropertyId(int prpId) {
		Integer srcId = getSourceIdForPropertyId(prpId);
		
		if(srcId==null)
			return null;

		return getSourceNameById(srcId);
	}
	
	public Context getContextForPropertyId(int prpId) {
		Integer srcId = getSourceIdForPropertyId(prpId);
		
		if(srcId==null)
			return null;
		
		return getContextForSourceId(srcId);
	}
	
}
