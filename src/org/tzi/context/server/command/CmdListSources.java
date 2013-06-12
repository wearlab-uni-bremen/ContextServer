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

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.tzi.context.common.Context;
import org.tzi.context.common.Util;


public class CmdListSources implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		Context ctx = null;
		Set<Integer> ctxSet = new TreeSet<Integer>();

		for(int wi=1; wi<scp.words.length; wi++) {
			ctxId = scp.csi.getCtxId(scp.words[wi]);

			if(ctxId!=null) {
				ctx = scp.contextMaps.getContextById(ctxId);
			} else {
				ctx = null;
			}

			if(ctx==null) {
				scr.commandError = true;
				scr.errorMsg = "Unknown or Invalid Context Identifier";
				break;
			} else {
				ctxSet.add(ctxId);
			}
		}
		if(!scr.commandError && ctxSet.isEmpty()) {
			ctxSet.addAll(scp.contextMaps.getContextMap().keySet());
			ctxSet.remove(-1);
		}
		if(!scr.commandError) {
			scr.reply.append(Integer.toString(ctxSet.size()));
			scr.reply.append(" ");
			for(Iterator<Integer> sctxidi = ctxSet.iterator(); sctxidi.hasNext(); ) {
				Integer sctxid = sctxidi.next();
				ctx = scp.contextMaps.getContextById(sctxid);
				// the server sources may contain sources that are not
				// yet merged with the context
				Map<Integer, String> serverCtxSources = scp.contextMaps.getSourceIdNameMapForContextId(sctxid);
				if(serverCtxSources==null)
					serverCtxSources = new TreeMap<Integer, String>();

				Set<String> ctxSources = ctx.getSources();
				scr.reply.append(sctxid.toString());
				scr.reply.append(" ");
				scr.reply.append(Integer.toString(serverCtxSources.size()));
				for(Iterator<Map.Entry<Integer, String>> sidei = serverCtxSources.entrySet().iterator(); sidei.hasNext(); ) {
					scr.reply.append(" ");
					Map.Entry<Integer, String> side = sidei.next();
					scr.reply.append(side.getKey().toString());
					if(!ctxSources.contains(side.getValue()))
						scr.reply.append("*");
					scr.reply.append("=");
					scr.reply.append(Util.urlencode(side.getValue()));
				}
				if(sctxidi.hasNext())
					scr.reply.append(" ");
			}
		}
		return scr;
	}

}
