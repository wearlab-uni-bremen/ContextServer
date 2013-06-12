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

public class CmdDeleteProperty implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer prpId = null;
		Integer srcId = null;
		Integer ctxId = null;
		scr.usage = "Usage: DELETEPRP (<ctxId> <srcId> <prpId>| <srcIdN> <prpN> | <prpN> )";
		if(scp.words.length < 2 || scp.words.length > 4 )
		{
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			if(scp.words.length == 2) {
				prpId = scp.csi.getPrpId(null, scp.words[1]);
				if(prpId!=null) {
					srcId = scp.contextMaps.getSourceIdForPropertyId(prpId);
					ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
				} else {
					scr.commandError = true;
					scr.errorMsg = "Invalid source ID!";
				}
			} else if(scp.words.length == 3) {
				srcId = scp.csi.getSrcId(null, scp.words[1]);
				prpId = scp.csi.getPrpId(srcId, scp.words[2]);
			} else {
				ctxId = scp.csi.getCtxId(scp.words[1]);
				srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
				prpId = scp.csi.getPrpId(srcId, scp.words[3]);
			}
			
			if(ctxId == null || srcId == null || prpId == null) {
				scr.commandError = true;
				scr.errorMsg = "Invalid context-, source- or property ID!";  
			} else {
				scp.csi.removeProperty(prpId);
				
				scr.writeOK = true;
			}
		}
		return scr;
	}


}
