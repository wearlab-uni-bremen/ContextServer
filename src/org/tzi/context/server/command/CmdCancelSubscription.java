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

public class CmdCancelSubscription implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		scr.usage = "CANCELSUB (all | (<sId> [<sId>...])";
		if(scp.words.length<2) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			scr.writeOK = true;
			if(scp.words[1].equalsIgnoreCase("all")) {
				scp.csm.removeAllClientSubscriptions(scp.cr.getId());
			} else {

				for(int i=1; i<scp.words.length; i++) {
					try {
						Integer sId = Integer.parseInt(scp.words[i]);
						scp.csm.removeClientSubscription(scp.cr.getId(), sId);
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = scr.usage;
					}
				}
			}
		}
		return scr;
	}

}
