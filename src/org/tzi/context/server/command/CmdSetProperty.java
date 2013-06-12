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

public class CmdSetProperty implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		scr.usage = "Usage: SETPRP (<ctxId> <srcId> <propId> | <srcIdN> <propId> | <propIdN>) = <value> <timestamp> <numtags> [tag1 tag2...] [isPersistent]";
		if(scp.words.length<6) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;									
		} else {
			ctxId = null;
			Integer srcId = null;
			Integer prpId = null;
			int vindex = -1;
			if("=".equalsIgnoreCase(scp.words[2])) {
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
				vindex = 3;
			} else {
				if("=".equalsIgnoreCase(scp.words[3])) {
					srcId = scp.csi.getSrcId(null, scp.words[1]);
					prpId = scp.csi.getPrpId(srcId, scp.words[2]);
					ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
					vindex = 4;
				} else {
					if("=".equalsIgnoreCase(scp.words[4]) && scp.words.length > 6) {
						ctxId = scp.csi.getCtxId(scp.words[1]);
						srcId = scp.csi.getSrcId(ctxId, scp.words[2]);
						prpId = scp.csi.getPrpId(srcId, scp.words[3]);
						vindex = 5;
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
					String value = scp.words.length > vindex ? scp.words[vindex++] : "";
					String timestampS = scp.words.length > vindex ? scp.words[vindex++] : "";
					String numtagsS = scp.words.length > vindex ? scp.words[vindex++] : "";
					long timestamp = 0L;
					int numtags = 0;

					try {
						timestamp = Long.parseLong(timestampS);
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = "Invalid Timestamp!";
					}

					if(timestamp == -1)
						timestamp = System.currentTimeMillis();

					if(!scr.commandError) {
						try {
							numtags = Integer.parseInt(numtagsS);
							if(numtags<0)
								throw new NumberFormatException();

						} catch(NumberFormatException nfe) {
							scr.commandError = true;
							scr.errorMsg = "Invalid Number of Tags!";
						}
					}

					if(!scr.commandError) {
						if(vindex + numtags > scp.words.length) {
							scr.commandError = true;
							scr.errorMsg = "Number or tags not matched!";
						}
					}

					if(!scr.commandError) {
						String [] tags = new String [numtags];

						for(int i=0; i<numtags; i++) {
							tags[i] = scp.words[vindex++];
						}

						boolean persistent = false;
						if(vindex<scp.words.length) {
							if(scp.words[vindex].startsWith("p") || scp.words[vindex].startsWith("P"))
								persistent = true;
						}
						String srcName = scp.contextMaps.getSourceNameById(srcId);
						String prpName = scp.contextMaps.getPropertyNameById(prpId);
						ContextElement ce = new ContextElement(srcName, prpName, value, timestamp, persistent, tags);
						scp.contextMaps.getContextById(ctxId).mergeContextElement(ce);
						scp.csi.propertyChange(prpId, ce);
						
						scr.writeOK = true;
					}
				}
			}
		}
		return scr;
	}

}
