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

import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.tzi.context.common.ContextServerInterface;
import org.tzi.context.common.HistoryProvider;
import org.tzi.context.common.Protocol.Command;
import org.tzi.context.server.ClientMaps;
import org.tzi.context.server.ClientRepresentation;
import org.tzi.context.server.ContextMaps;
import org.tzi.context.server.ContextSubscriptionManager;

public class ServerCommandParameters {
	public ContextServerInterface csi;
	public ContextSubscriptionManager csm;
	public ClientRepresentation cr;
	public ScriptEngineManager sem;
	public ScriptEngineFactory sef;
	
	public String [] words;
	public String commandS;
	public Command command;
	
	public ClientMaps clientMaps;
	public ContextMaps contextMaps;
	
	public HistoryProvider history;
}
