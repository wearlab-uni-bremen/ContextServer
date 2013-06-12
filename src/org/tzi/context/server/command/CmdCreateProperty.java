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

public class CmdCreateProperty implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		scr.usage = "Usage: CREATEPRP (<ctxId> <srcId> | <srcIdN>) <propname>";
		switch(scp.words.length) {
		case 4:
			ctxId = scp.csi.getCtxId(scp.words[1]);
			if(ctxId==null || ctxId==-1) {
				scr.commandError = true;
				scr.errorMsg = "Unknown or Invalid Context Identifier";
			} else {
				Integer srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
				if(srcId==null || srcId==-1) {
					scr.commandError = true;
					scr.errorMsg = "Unknown or Invalid Source Identifier";
				} else {
					Integer prpId = scp.csi.addProperty(srcId, scp.words[3]);
					scr.reply.append(prpId.toString()); 
				}
			}
			break;
		case 3:
			Integer srcId = scp.csi.getSrcId(null, scp.words[1]);
			if(srcId==null || srcId==-1) {
				scr.commandError = true;
				scr.errorMsg = "Unknown or Invalid Source Identifier";
			} else {
				Integer prpId = scp.csi.addProperty(srcId, scp.words[2]);
				scr.reply.append(prpId.toString()); 
			}
			break;
		default:
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		}
		return scr;
	}

}
