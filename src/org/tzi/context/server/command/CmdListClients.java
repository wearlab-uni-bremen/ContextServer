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

import org.tzi.context.common.Util;
import org.tzi.context.server.ClientRepresentation;


public class CmdListClients implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult result = new ServerCommandResult();
		
		result.reply.append(Integer.toString(scp.clientMaps.idClientMap.size()));
		result.reply.append(" ");
		for(Iterator<Map.Entry<Integer, ClientRepresentation>> cltei = scp.clientMaps.idClientMap.entrySet().iterator(); cltei.hasNext(); ) {
			Map.Entry<Integer, ClientRepresentation> clte = cltei.next();
			result.reply.append(clte.getKey());
			result.reply.append("=");
			result.reply.append(Util.urlencode(clte.getValue().getName()));
			if(cltei.hasNext())
				result.reply.append(", ");
		}
		return result;
	}

}
