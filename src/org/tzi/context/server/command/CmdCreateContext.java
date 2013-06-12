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

import org.tzi.context.common.Context;
import org.tzi.context.server.ClientProxyListener;


public class CmdCreateContext implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		Context ctx = null;
		
		if(scp.words.length<2)
		{
			scr.commandError = true;
			scr.errorMsg = "Usage: CREATECTX <name>";
		} else {
			ctxId = scp.csi.addContext(scp.words[1]);
			ctx = scp.contextMaps.getContextById(ctxId);
			assert(ctx!=null);

			// check for waiting listeners
			List<ClientProxyListener> pendingListener = scp.contextMaps.pendingContextListener.get(scp.words[1]);

			if(pendingListener!=null) {
				for(ClientProxyListener cl : pendingListener) {
					ctx.addContextListener(cl);
				}

				scp.contextMaps.pendingContextListener.remove(scp.words[1]);
			}

			// check for listeners waiting for all contexts
			pendingListener = scp.contextMaps.pendingContextListener.get(Context.ALL_CONTEXTS);

			if(pendingListener!=null) {
				for(ClientProxyListener cl : pendingListener) {
					ctx.addContextListener(cl);
				}
			}

			scr.reply.append(ctxId.toString());
		}
		return scr;
	}

}
