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

public class CmdGetSourceID implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		String srcName = null;
		scr.usage = "Usage: GETSRCID <ctxId> <sourcename>";
		if(scp.words.length!=3) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			ctxId = scp.csi.getCtxId(scp.words[1]);
			if(ctxId==null || ctxId==-1) {
				scr.commandError = true;
				scr.errorMsg = "Unknown or Invalid Context Identifier";
			} else {
				srcName = scp.words[2];
				Integer srcId = scp.contextMaps.getSourceIdByName(ctxId, srcName);
				if(srcId==null || srcId == -1) {
					scr.commandError = true;
					scr.errorMsg = "Unknown source!";
				} else {
					scr.reply.append(Integer.toString(srcId));
				}
			}			
		}
		return scr;
	}

}
