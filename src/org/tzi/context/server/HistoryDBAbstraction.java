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

import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

import org.tzi.context.abstractenv.AbstractItem;
import org.tzi.context.abstractenv.ContextAbstraction;
import org.tzi.context.abstractenv.PropertyAbstraction;
import org.tzi.context.abstractenv.SourceAbstraction;
import org.tzi.context.common.ContextElement;

/**
 * An abstraction for storing and retrieving older values from
 * properties.<br>
 * This abstract class offers housekeeping functionalities mostly used by the
 * server but also available to implementors of actual storage functionality.<br>
 * Implementors only need to implement 4 abstract methods for actual data retrieval
 * but other methods can be overridden as needed.<br>
 * Since elements on the server can be removed and recreated the history is expected to
 * keep track of an element being active or not (present on the server) and store this 
 * information. This class provides methods to help with this task.
 * @author hendrik
 *
 */
public abstract class HistoryDBAbstraction {
	
	protected static final Boolean bTrue = true;
	protected static final Boolean bFalse = false;
	
	protected static final String dActive = "_db_active";
	
	protected HistoryDBEnvironment dbenv = new HistoryDBEnvironment();
	
	private Properties props;
	
	public HistoryDBAbstraction(Properties p) {
		this.props = p;
	}
	
	
	public String getProperty(String key) {
		return props.getProperty(key);
	}

	public String getProperty(String key, String def) {
		return props.getProperty(key, def);
	}

	public static interface HistoryDBCheck {
		public boolean checkDatabase(HistoryDBAbstraction hdb);
	}
	
	public Set<Integer> getContextIds() {
		TreeSet<Integer> s = new TreeSet<Integer>();
		for(ContextAbstraction ca : dbenv.getContexts())
			s.add(ca.getId());
		
		return s;
	}
	
	public Integer getContextId(String contextName) {
		ContextAbstraction ca = dbenv.getContextByName(contextName);
		return ca == null ? null : ca.getId();
	}
	
	public String getContextName(Integer contextId) {
		ContextAbstraction ca = dbenv.getContextById(contextId);
		return ca == null ? null : ca.getName();
	}
	
	protected boolean activeItem(AbstractItem ai) {
		if(ai == null)
			return false;
		
		Boolean bAct = (Boolean)ai.getData(dActive);
		
		return bAct == null ? true : bAct.booleanValue();
	}
	
	protected void setActiveItem(AbstractItem ai, boolean active) {
		if(ai == null)
			return;
		
		ai.setData(dActive, active ? bTrue : bFalse);
	}

	protected ContextAbstraction getCAByName(String contextName) {
		if(contextName == null)
			return null;
		
		return dbenv.getContextByName(contextName);
	}
	
	protected Collection<ContextAbstraction> getCAs() {
		return dbenv.getContexts();
	}
	
	protected ContextAbstraction getCA(Integer ctxId) {
		if(ctxId == null)
			return null;
		
		return dbenv.getContextById(ctxId);
	}
	
	protected SourceAbstraction getSA(Integer ctxId, Integer srcId) {
		if(ctxId == null)
			return null;
		if(srcId == null)
			return null;
		
		ContextAbstraction ca = dbenv.getContextById(ctxId);
		
		if(ca == null)
			return null;
		
		return ca.getSourceById(srcId);
	}
	
	protected SourceAbstraction getSAByName(String contextName, String sourceName) {
		ContextAbstraction ca = getCAByName(contextName);
		
		return ca == null ? null : ca.getSourceByName(sourceName);
	}
	
	protected PropertyAbstraction getPAByName(String contextName, String sourceName, String propertyName) {
		SourceAbstraction sa = getSAByName(contextName, sourceName);
		return sa == null ? null : sa.getPropertyByName(propertyName);
	}
	
	protected PropertyAbstraction getPA(Integer ctxId, Integer srcId, Integer prpId) {
		if(ctxId == null)
			return null;
		if(srcId == null)
			return null;
		if(prpId == null)
			return null;
		
		ContextAbstraction ca = dbenv.getContextById(ctxId);
		
		if(ca == null)
			return null;
		
		SourceAbstraction sa = ca.getSourceById(srcId);
		
		if(sa == null)
			return null;
		
		return sa.getPropertyById(prpId);
	}
	
	public Integer addContext(String contextName) {
		ContextAbstraction ca = dbenv.createContext(contextName);
		setActiveItem(ca, true);
		return ca.getId();
	}
	
	public Integer addSource(String contextName, String sourceName) {
		ContextAbstraction ca = dbenv.createContext(contextName);
		setActiveItem(ca, true);
		SourceAbstraction sa = dbenv.createSource(ca, sourceName);
		setActiveItem(sa, true);
		return sa.getId();
	}
	public Integer addProperty(String contextName, String sourceName, String propertyName) {
		ContextAbstraction ca = dbenv.createContext(contextName);
		setActiveItem(ca, true);
		SourceAbstraction sa = dbenv.createSource(ca, sourceName);
		setActiveItem(sa, true);
		PropertyAbstraction pa = dbenv.createProperty(sa, propertyName);
		setActiveItem(pa, true);
		return pa.getId();
	}
	
	public void removeProperty(String contextName, String sourceName, String propertyName) {
		PropertyAbstraction pa = getPAByName(contextName, sourceName, propertyName);
		if(pa==null)
			return;
		
		setActiveItem(pa, false);
	}
	public void removeSource(String contextName, String sourceName) {
		SourceAbstraction sa = getSAByName(contextName, sourceName);
		if(sa==null)
			return;
		
		setActiveItem(sa, false);
	}
	
	public void removeContext(String contextName) {
		ContextAbstraction ca = getCAByName(contextName);
		if(ca == null)
			return;
		
		setActiveItem(ca, false);
	}
	
	public Set<String> getActiveContexts() {
		Set<String> ac = new TreeSet<String>();
		
		for(ContextAbstraction ca : dbenv.getContexts()) {
			if(activeItem(ca)) {
				ac.add(ca.getName());
			}
		}
		
		return ac;
	}
	
	public Set<String> getActiveSources(String context) {
		Set<String> as = new TreeSet<String>();
		
		ContextAbstraction ca = getCAByName(context);
		
		if(ca != null) {
		for(SourceAbstraction sa : ca.getSources()) {
			if(activeItem(sa)) {
				as.add(sa.getName());
			}
		}
		}
		
		return as;
	}
	
	public Set<String> getActiveProperties(String context, String source) {
		Set<String> ap = new TreeSet<String>();
		
		SourceAbstraction sa = getSAByName(context, source);
		
		if(sa!=null) {
			for(PropertyAbstraction pa : sa.getProperties()) {
				if(activeItem(pa)) {
					ap.add(pa.getName());
				}
			}
		}
		
		return ap;
	}
	
	public boolean initialize() { return true; };

	public abstract void propertyChange(String contextName, String sourceName, String propertyName, long timestamp, String value, Set<String> tags, boolean persistent);
	
	public abstract ContextElement getLastState(String context, String source, String property);

	public abstract long getPrpTimestamp(String context, String source, String property, boolean getMax);

	/**
	 * 
	 * @param context
	 * @param source
	 * @param property
	 * @param limit
	 * @param from
	 * @param to
	 * @param withTags set to <em>null</em> to allow all tags, else only elements with at least one matching tag are included
	 * @param l
	 * @return
	 */
	public abstract List<ContextElement> getHistory(String context, String source, String property, int limit, long from, long to, Set<String> withTags, List<ContextElement> l);
}