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


import org.tzi.context.common.Context;

import org.tzi.context.common.ContextElement;
import org.tzi.context.common.ContextListener;
import org.tzi.context.common.ContextListenerInterface;
import org.tzi.context.common.ContextMessage;
import org.tzi.context.common.ContextServerInterface;

public class ClientProxyListener implements ContextListener {
	
	private ContextServerInterface ctxSI;
	private ClientRepresentation cr;
	private Integer listenerId;
	private ContextListenerInterface properties = null;
	
	private boolean useShortFormat = false;
	
	public boolean isShortFormat() {
		return useShortFormat;
	}
	
	public void setShortFormat(boolean sf) {
		useShortFormat = sf;
	}
	
	public ClientProxyListener(ContextServerInterface ctxSI, Integer listenerId, ContextListenerInterface clp, ClientRepresentation cr) {
		this.ctxSI = ctxSI;
		this.listenerId = listenerId;
		this.properties = clp;
		this.cr = cr;
	}
	
	public ClientProxyListener(ContextServerInterface ctxSI, Integer listenerId, ClientRepresentation cr) {
		this(ctxSI, listenerId, null, cr);
	}
	
	public Integer getId() {
		return listenerId;
	}
	
	public void setId(Integer id) {
		this.listenerId = id;
	}
	
	public ContextListenerInterface getProperties() {
		return properties;
	}
	
	public void setCLP(ContextListenerInterface clp) {
		properties = clp;
	}

	public void processContext(Context ctx, ContextElement ce) {
		if(properties.matches(ce)) {
			Integer ctxId = ctxSI == null ? null : ctxSI.getContextId(ctx);
			String info = ctxId == null ? null : Integer.toString(ctxId);
			ContextMessage cm = new ContextMessage(Integer.toString(listenerId), ctx.getName(), info, ce);
			if(isShortFormat()) {
				Integer srcId = ctxId == null ? null : ctxSI.getSrcId(ctxId, "@"+ce.getSourceIdentifier());
				Integer prpId = srcId == null ? null : ctxSI.getPrpId(srcId, "@"+ce.getPropertyIdentifier());
				if(prpId!=null) {
					cm.setShortFormat(true);
					cm.setShortPrefix(Integer.toString(prpId));
				}
			}
			
			cr.addMessage(cm.toString());
		}
	}

	public void propertyAdded(Context ctx, String source, String property) {
		if(properties.notifyNewProperty(source, property)) {
			Integer ctxId = ctxSI == null ? null : ctxSI.getContextId(ctx);
			String info = ctxId == null ? null : Integer.toString(ctxId);
			cr.addMessage(new ContextMessage(Integer.toString(listenerId), ctx.getName(), info, source, property, true).toString());
		}
	}

	public void propertyRemoved(Context ctx, String source, String property) {
		if(properties.notifyNewProperty(source, property)) {
			Integer ctxId = ctxSI == null ? null : ctxSI.getContextId(ctx);
			String info = ctxId == null ? null : Integer.toString(ctxId);
			cr.addMessage(new ContextMessage(Integer.toString(listenerId), ctx.getName(), info, source, property, false).toString());
		}
	}

	public void sourceAdded(Context ctx, String source, String property) {
		if(properties.notifyNewSource(source)) {
			Integer ctxId = ctxSI == null ? null : ctxSI.getContextId(ctx);
			String info = ctxId == null ? null : Integer.toString(ctxId);
			cr.addMessage(new ContextMessage(Integer.toString(listenerId), ctx.getName(), info, source, property).toString());
		}
	}

	public void sourceRemoved(Context ctx, String source) {
		if(properties.notifyNewSource(source)) {
			Integer ctxId = ctxSI == null ? null : ctxSI.getContextId(ctx);
			String info = ctxId == null ? null : Integer.toString(ctxId);
			cr.addMessage(new ContextMessage(Integer.toString(listenerId), ctx.getName(), info, source, null).toString());
		}
	}

}
