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

import org.tzi.context.common.Context;
import org.tzi.context.common.Util;


public class CmdListContexts implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		scr.reply.append(Integer.toString(scp.contextMaps.getContextMap().size()-1));
		scr.reply.append(" ");
		for(Iterator<Map.Entry<Integer, Context>> ctxei = scp.contextMaps.getContextMap().entrySet().iterator(); ctxei.hasNext(); ) {
			Map.Entry<Integer, Context> ctxe = ctxei.next();
			if(ctxe.getKey()==-1) // skip <All>
				continue;
			scr.reply.append(ctxe.getKey());
			scr.reply.append("=");
			scr.reply.append(Util.urlencode(ctxe.getValue().getName()));
			if(ctxei.hasNext())
				scr.reply.append(" ");
		}
		return scr;
	}

}
