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
import java.util.Vector;

import org.tzi.context.common.Context;
import org.tzi.context.common.ContextListener;
import org.tzi.context.server.ClientProxyListener;


public class CmdDeleteContext implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		Context ctx = null;
		scr.usage = "Usage: DELETECTX CTXID";
		if(scp.words.length<2)
		{
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			ctxId = scp.csi.getCtxId(scp.words[1]);

			if(ctxId != null) {
				ctx = scp.contextMaps.getContextById(ctxId);
			}

			if(ctx==null) {
				scr.commandError = true;
				scr.errorMsg = "Invalid context ID";
			} else {
				scp.csi.removeContext(ctxId);

				// save old proxy listeners to restore
				// subscription later.
				if(ctx.getContextListeners().size() > 0) {
					List<ClientProxyListener> cpll = scp.contextMaps.pendingContextListener.get(ctx.getName());
					if(cpll == null) {
						cpll = new Vector<ClientProxyListener>();
						scp.contextMaps.pendingContextListener.put(ctx.getName(), cpll);
					}
					for(ContextListener cl : ctx.getContextListeners()) {
						if(cl instanceof ClientProxyListener) {
							cpll.add((ClientProxyListener)cl);
						}
					}
					if(cpll.size() == 0) {
						scp.contextMaps.pendingContextListener.remove(ctx.getName());
					}
				}

				scr.writeOK = true;
			}
		}
		return scr;
	}
}
