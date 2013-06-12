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

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.tzi.context.abstractenv.ContextAbstraction;
import org.tzi.context.abstractenv.PropertyAbstraction;
import org.tzi.context.abstractenv.PropertyValues;
import org.tzi.context.abstractenv.SourceAbstraction;
import org.tzi.context.common.ContextElement;

public class MemoryHistory extends HistoryDBAbstraction {

	private static final String dHistory = "_d_mem_history";
	public static final String propMaxItems = "org.tzi.context.server.MemoryHistory.maxItems";
	public static final int propMaxItemsDefault = 50;
	
	private int maxItems;
	
	public MemoryHistory(Properties props) {
		super(props);
		
		maxItems = Integer.parseInt(props.getProperty(propMaxItems, Integer.toString(propMaxItemsDefault)));
		if(maxItems<0)
			maxItems = 0;
	}
	
	@Override
	public void propertyChange(String contextName, String sourceName,
			String propertyName, long timestamp, String value,
			Set<String> tags, boolean persistent) {
		PropertyAbstraction pa = getPAByName(contextName, sourceName, propertyName);
		if(pa==null) {
			ContextAbstraction ca = dbenv.createContext(contextName);
			SourceAbstraction sa = dbenv.createSource(ca, sourceName);
			pa = dbenv.createProperty(sa, propertyName);
		}
		setActiveItem(pa, true);
		
		@SuppressWarnings("unchecked")
		LinkedList<PropertyValues> hl = (LinkedList<PropertyValues>)pa.getData(dHistory);
		if(hl==null) {
			pa.setData(dHistory, hl = new LinkedList<PropertyValues>());
		}
		PropertyValues pv = new PropertyValues(timestamp, value, tags, persistent);
		pa.setValues(pv);
		hl.add(pv);
		while(hl.size()>maxItems) {
			hl.removeFirst();
		}
	}
	

	@Override
	public ContextElement getLastState(String context, String source,
			String property) {
		PropertyAbstraction pa = getPAByName(context, source, property);
		if(pa==null)
			return null;
		PropertyValues pv = pa.getValues();
		if(pv==null)
			return null;

		return new ContextElement(source, property, pv.getValue(), pv.getTimestamp(), pv.isPersistent(), pv.getTags());
	}

	@Override
	public long getPrpTimestamp(String context, String source, String property,
			boolean getMax) {
		PropertyAbstraction pa = getPAByName(context, source, property);
		if(pa==null)
			return -1;
		
		@SuppressWarnings("unchecked")
		LinkedList<PropertyValues> hl = (LinkedList<PropertyValues>)pa.getData(dHistory);
		
		if(hl==null)
			return -1;
		
		if(getMax)
			return hl.getLast().getTimestamp();
		
		return hl.getFirst().getTimestamp();
	}

	@Override
	public List<ContextElement> getHistory(String context, String source,
			String property, int limit, long from, long to, Set<String> withTags,
			List<ContextElement> l) {
		
		LinkedList<ContextElement> cehist = new LinkedList<ContextElement>();
		
		PropertyAbstraction pa = getPAByName(context, source, property);
		if(pa==null)
			return cehist;
		
		@SuppressWarnings("unchecked")
		LinkedList<PropertyValues> hl = (LinkedList<PropertyValues>)pa.getData(dHistory);
		
		if(hl==null)
			return cehist;
		
		boolean fromTo = to != -1;
		
		LinkedList<PropertyValues> pvlist = new LinkedList<PropertyValues>();
		
		for(PropertyValues pv : hl) {
			long ts = pv.getTimestamp();
			if(ts >= from && ( (!fromTo) || (ts <= to))) {
				boolean canAdd = true;
				if(withTags != null) {
					// add if no tags wanted and none present
					canAdd = (pv.getTags().size() == 0 && withTags.size() == 0);
					// or check for matching tag
					for(String tag : withTags) {
						if(pv.getTags().contains(tag)) {
							canAdd = true;
							break;
						}
					}
				}
				if(canAdd)
					pvlist.add(pv);
			}
		}
		
		if(limit > 0) {
			while(pvlist.size()>limit) {
				pvlist.removeFirst();
			}
		}
		
		for(PropertyValues pv : pvlist) {
			cehist.add(new ContextElement(source, property, pv.getValue(), pv.getTimestamp(), pv.isPersistent(), pv.getTags()));
		}
		
		return cehist;
	}

}
