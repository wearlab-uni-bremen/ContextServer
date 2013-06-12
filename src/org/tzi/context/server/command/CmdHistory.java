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

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.tzi.context.common.ContextElement;
import org.tzi.context.common.Util;

public class CmdHistory implements ServerCommand {

	private static boolean isCmd(String s) {
		//if(s.equalsIgnoreCase("count"))
		//	return true;
		if(s.equalsIgnoreCase("get"))
			return true;
		if(s.equalsIgnoreCase("latest"))
			return true;
		if(s.equalsIgnoreCase("earliest"))
			return true;
		
		return false;
	}
	
	private static boolean isEarliest(String s) {
		return s.equalsIgnoreCase("earliest");
	}
	private static boolean isLatest(String s) {
		return s.equalsIgnoreCase("latest");
	}
	private static boolean isGet(String s) {
		return s.equalsIgnoreCase("get");
	}
	
	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		
		scr.usage = "Usage: HISTORY (<ctxId> <srcId> <propId> | <srcIdN> <propId> | <propIdN>) ((get [-]from [+|r]to limit [tags t1 t2 ...]) | (latest|earliest))"; 
		
		if(!scp.history.historySupported()) {
			scr.commandError = true;
			scr.errorMsg = "History not supported!";
		} else {
			// history (ctxId | srcId | prpId) ( (count|get) from to limit | (latest|earliest) )
			
			if(scp.words.length<3) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;									
			} else {
				Integer ctxId = null;
				Integer srcId = null;
				Integer prpId = null;
				int cmdIndex = -1;
				if(scp.words.length > 2 && isCmd(scp.words[2])) {
					prpId = scp.csi.getPrpId(null, scp.words[1]);
					if(prpId!=null) {
						srcId = scp.contextMaps.getSourceIdForPropertyId(prpId);
						if(srcId!=null) {
							ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
						}
						else {
							scr.commandError = true;
						}
					} else {
						scr.commandError = true;
					}
					cmdIndex = 2;
				} else {
					if(scp.words.length > 3 && isCmd(scp.words[3])) {
						srcId = scp.csi.getSrcId(null, scp.words[1]);
						prpId = scp.csi.getPrpId(srcId, scp.words[2]);
						ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
						cmdIndex = 3;
					} else {
						if(scp.words.length > 4 && isCmd(scp.words[4])) {
							ctxId = scp.csi.getCtxId(scp.words[1]);
							srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
							prpId = scp.csi.getPrpId(srcId, scp.words[3]);
							cmdIndex = 4;
						} else {
							scr.commandError = true;
							scr.errorMsg = scr.usage;
						}
					}
				}
				if(!scr.commandError) {
					if(prpId==null || prpId==-1) {
						scr.commandError = true;
						scr.errorMsg = "Unable to find property...";
					} else {
						String cmd = scp.words[cmdIndex];
						boolean rangeCommand = true;
						if(isEarliest(cmd)) {
							scr.reply.append(scp.history.earliestRecord(prpId));
							rangeCommand = false;
						}
						if(isLatest(cmd)) {
							scr.reply.append(scp.history.latestRecord(prpId));
							rangeCommand = false;
						}
						if(rangeCommand) {
							if( !isGet(cmd) || (scp.words.length - (cmdIndex+1)) < 3) {
								scr.commandError = true;
								scr.errorMsg = scr.usage;
							} else {
								try {
									String toS = scp.words[cmdIndex+2];
									boolean toRelative = toS.startsWith("+") || toS.startsWith("r");
									if(toRelative) {
										toS = toS.substring(1);
									}
									
									
									long from = Long.parseLong(scp.words[cmdIndex+1]);
									long to = Long.parseLong(toS);
									int limit = Integer.parseInt(scp.words[cmdIndex+3]);
									
									Set<String> withTags = null;
									
									if(scp.words.length > (cmdIndex+4) && scp.words[cmdIndex+4].equals("tags") ) {
										withTags = new TreeSet<String>();
										for(int i=cmdIndex+5; i<scp.words.length; i++) {
											withTags.add(scp.words[i]);
										}
									}
									
									long curTime = System.currentTimeMillis();
									
									if(from < 0) {
										from += (to == -1) ? curTime : to;
									} else {
										if(toRelative) {
											to += from;
										}
									}
									
									List<ContextElement> history = scp.history.getHistory(prpId, from, to, limit, withTags);
									
									scr.reply.append(Integer.toString(history.size()));
									
									for(ContextElement ce : history) {
										scr.reply.append(' ');
										scr.reply.append(Util.urlencode(ce.toShortString(null)));
									}
									
								} catch(NumberFormatException nfe) {
									scr.commandError = true;
									scr.errorMsg = scr.usage;
								}
							}
						}
					}
				}
			}
		}
		return scr;
	}

}
