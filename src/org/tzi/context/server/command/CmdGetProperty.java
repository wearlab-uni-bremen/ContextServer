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

import org.tzi.context.common.ContextElement;
import org.tzi.context.common.Util;

public class CmdGetProperty implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		scr.usage = "Usage: GETPRP (<ctxId> <srcId> <propId> | <srcIdN> <propId> | <propIdN>)";
		if(scp.words.length<2) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;									
		} else {
			ctxId = null;
			Integer srcId = null;
			Integer prpId = null;
			if(scp.words.length==2) {
				prpId = scp.csi.getPrpId(null, scp.words[1]);
				if(prpId!=null)
					srcId = scp.contextMaps.getSourceIdForPropertyId(prpId);
				if(srcId!=null)
					ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
			} else {
				if(scp.words.length==3) {
					srcId = scp.csi.getSrcId(null, scp.words[1]);
					if(srcId!=null)
						prpId = scp.csi.getPrpId(srcId, scp.words[2]);
					if(srcId!=null)
						ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
				} else {
					if(scp.words.length==4) {
						ctxId = scp.csi.getCtxId(scp.words[1]);
						if(ctxId!=null)
							srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
						if(srcId!=null)
							prpId = scp.csi.getPrpId(srcId, scp.words[3]);
					} else {
						scr.commandError = true;
						scr.errorMsg = scr.usage;
					}
				}
			}
			if(!scr.commandError) {
				if(prpId==null) {
					scr.commandError = true;
					scr.errorMsg = "Unable to find property...";
				} else {
					String srcName = scp.contextMaps.getSourceNameById(srcId);
					String prpName = scp.contextMaps.getPropertyNameById(prpId);
					ContextElement ce = scp.contextMaps.getContextById(ctxId).getSourceProperty(srcName, prpName);
					if(ce==null) {
						// property not yet defined
						scr.reply.append("-1");
					} else {
						String v = (ce==null || ce.getValue()==null)?"":ce.getValue();

						if(v.length()>0) {
							scr.reply.append("1 ");
							scr.reply.append(Util.urlencode(v));
							scr.reply.append(" ");
						} else {
							scr.reply.append("0 ");
						}

						scr.reply.append(Long.toString(ce.getTimestamp()));
						scr.reply.append(" ");

						scr.reply.append(Integer.toString(ce.getTypeTags().size()));

						scr.reply.append(" ");
						for(String t : ce.getTypeTags()) {
							scr.reply.append(Util.urlencode(t));
							scr.reply.append(" ");
						}

						scr.reply.append(ce.isPersistent()?"1":"0");
					}
				}
			}
		}
		return scr;
	}

}
