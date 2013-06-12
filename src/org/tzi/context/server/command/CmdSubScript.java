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
package org.tzi.context.server.command;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import org.tzi.context.common.Context;
import org.tzi.context.common.ContextListenerScript;
import org.tzi.context.server.ClientProxyListener;


public class CmdSubScript implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		String ctxName = null;
		Set<Integer> listenerIds = new TreeSet<Integer>();
		scr.usage = "Usage: SUBSCRIPT <ctxId> <Scriptdata>";
		if(scp.words.length<3) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			ctxId = scp.csi.getCtxId(scp.words[1]);
			ctxName = scp.csi.getStringId(scp.words[1]);
			Context ctx = ctxId == null ? null : scp.contextMaps.getContextById(ctxId);

			if(ctxId == null || (ctxId != -1 && ctx == null)) {
				scr.commandError = true;
				scr.errorMsg = "Invalid context specified!";
			} else {
				StringBuilder scriptBuilder = new StringBuilder();
				for(int i=2; i<scp.words.length; i++)
					scriptBuilder.append(scp.words[i]);
				ScriptEngine engine = scp.sef.getScriptEngine();
				assert(engine instanceof Invocable); // checked by server
				try {
					engine.eval(scriptBuilder.toString());
					ContextListenerScript cls = new ContextListenerScript((Invocable)engine);
					ClientProxyListener cpl = new ClientProxyListener(scp.csi, -1, cls, scp.cr);
					listenerIds.add(scp.csm.addClientProxyListener(scp.cr.getId(), cpl));
					if(ctx!=null) {
						ctx.addContextListener(cpl);
					} else {
						
						if(ctxId != null && ctxId == -1) {
							for(Context actx : scp.contextMaps.getContextMap().values()) {
								if(actx != null) { // there is a null-context...
									actx.addContextListener(cpl);
								}
							}
							ctxName = Context.ALL_CONTEXTS;
						} 

						List<ClientProxyListener> cll = scp.contextMaps.pendingContextListener.get(ctxName);
						if(cll==null) {
							cll = new Vector<ClientProxyListener>();
							scp.contextMaps.pendingContextListener.put(ctxName, cll);
						}
						cll.add(cpl);

					}
				} catch (ScriptException e) {
					scr.commandError = true;
					scr.errorMsg = "Error parsing script at line " + e.getLineNumber();
				}
			}
		}
		return scr;
	}
}
