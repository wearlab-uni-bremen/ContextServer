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

import org.tzi.context.common.Context;
import org.tzi.context.common.Util;

public class CmdGetIDInfo implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer id = null;
		scr.usage = "Usage: GETIDINFO <Id>";
		if(scp.words.length!=2) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			try  {
				id = Integer.parseInt(scp.words[1]);
				scr.reply.append("IDINFO ");
				
				Context ctx = scp.contextMaps.getContextById(id);
				
				if(ctx != null) {
					scr.reply.append("C ");
					scr.reply.append(Integer.toString(id));
					scr.reply.append(" ");
					scr.reply.append(Util.urlencode(ctx.getName()));
				} else {
					String srcName = scp.contextMaps.getSourceNameById(id);
					
					if(srcName!=null) {
						Integer ctxId = scp.contextMaps.getContextIdForSourceId(id);
						assert(ctxId!=null);
						ctx = scp.contextMaps.getContextById(ctxId);
						assert(ctx != null);
						
						scr.reply.append("S ");
						scr.reply.append(Integer.toString(id));
						scr.reply.append(" ");
						scr.reply.append(Util.urlencode(srcName));
						scr.reply.append(" C ");
						scr.reply.append(Integer.toString(ctxId));
						scr.reply.append(" ");
						scr.reply.append(Util.urlencode(ctx.getName()));
					} else {
						String prpName = scp.contextMaps.getPropertyNameById(id);
						
						if(prpName != null) {
							Integer srcId = scp.contextMaps.getSourceIdForPropertyId(id);
							assert(srcId != null);
							srcName = scp.contextMaps.getSourceNameById(srcId);
							assert(srcName!=null);
							Integer ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
							assert(ctxId!=null);
							ctx = scp.contextMaps.getContextById(ctxId);
							assert(ctx!=null);
							
							scr.reply.append("P ");
							scr.reply.append(Integer.toString(id));
							scr.reply.append(" ");
							scr.reply.append(Util.urlencode(prpName));
							scr.reply.append(" S ");
							scr.reply.append(Integer.toString(srcId));
							scr.reply.append(" ");
							scr.reply.append(Util.urlencode(srcName));
							scr.reply.append(" C ");
							scr.reply.append(Integer.toString(ctxId));
							scr.reply.append(" ");
							scr.reply.append(Util.urlencode(ctx.getName()));
						} else {
							scr.reply.append("U ");
							scr.reply.append(Integer.toString(id));
						}
					}
				}
				
			} catch(NumberFormatException nfe) {
				scr.commandError = true;
				scr.errorMsg = "Invalid ID given!";
			}
		}
		return scr;
	}

}
