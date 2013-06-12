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

public class CmdShortSubscription implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		scr.usage = "SHORTSUB (true|false) (all | (<sId> [<sId>...])";
		if(scp.words.length<3) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			boolean state = Boolean.parseBoolean(scp.words[1]);
			scr.writeOK = true;
			if(scp.words[2].equalsIgnoreCase("all")) {
				scp.csm.setShortStateAllClientSubscription(scp.cr.getId(), state);
			} else {
				for(int i=2; i<scp.words.length; i++) {
					try {
						Integer sId = Integer.parseInt(scp.words[i]);
						scp.csm.setShortStateClientSubscription(scp.cr.getId(), sId, state);
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
