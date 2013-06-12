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

public class CmdDeleteSource implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer srcId = null;
		Integer ctxId = null;
		scr.usage = "Usage: DELETESRC (<ctxId> <srcId> | <srcIdN>)";
		if(scp.words.length != 2 && scp.words.length != 3)
		{
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			if(scp.words.length == 2) {
				srcId = scp.csi.getSrcId(null, scp.words[1]);
				if(srcId!=null) {
					ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
				} else {
					scr.commandError = true;
					scr.errorMsg = "Invalid source ID!";
				}
			} else {
				ctxId = scp.csi.getCtxId(scp.words[1]);
				srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
			}
			
			if(ctxId == null || srcId == null) {
				scr.commandError = true;
				scr.errorMsg = "Invalid context- or source ID!";  
			} else {
				scp.csi.removeSource(srcId);
				
				scr.writeOK = true;
			}
		}
		return scr;
	}


}