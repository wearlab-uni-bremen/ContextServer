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
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.tzi.context.common.Context;
import org.tzi.context.common.Util;


public class CmdListProperties implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		Context ctx = null;
		Map<Integer, Set<Integer>> ctxIdSrcIdSetMap = new TreeMap<Integer, Set<Integer>>();

		wordloop: for(int wi=1; wi<scp.words.length; wi++) {
			ctxId = scp.csi.getCtxId(scp.words[wi]);

			if(ctxId!=null) {
				ctx = scp.contextMaps.getContextById(ctxId);
			} else {
				ctx = null;
			}

			if(ctx==null) {
				scr.commandError = true;
				scr.errorMsg = "Unknown or Invalid Context Identifier";
				break;
			} else {
				wi++;
				if(wi>=scp.words.length) {
					scr.commandError = true;
					scr.errorMsg = "Missing Number of Sources";
					break;
				} else {
					int numSources;
					try {
						numSources = Integer.parseInt(scp.words[wi]);
						if(numSources<0)
							throw new NumberFormatException();
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = "Invalid Number of Sources";
						break;
					}

					Set<Integer> sources = new TreeSet<Integer>();
					for(int si=0; si<numSources; si++) {
						wi++;
						if(wi<scp.words.length) {
							Integer sSrcId = scp.csi.getSrcId(ctxId, scp.words[wi]);
							if(sSrcId==null || sSrcId==-1) {
								scr.commandError = true;
								scr.errorMsg = "Invalid Source Identifier";
								break wordloop;
							}
							sources.add(sSrcId);
						}
					}

					ctxIdSrcIdSetMap.put(ctxId, sources);
				}
			}
		}
		if(!scr.commandError) {
			scr.reply.append(Integer.toString(ctxIdSrcIdSetMap.size()));
			scr.reply.append(" ");
			for(Iterator<Map.Entry<Integer, Set<Integer>>> ctxsourcesei = ctxIdSrcIdSetMap.entrySet().iterator(); ctxsourcesei.hasNext(); ) {
				Map.Entry<Integer, Set<Integer>> ctxsourcese = ctxsourcesei.next();
				Integer sctxid = ctxsourcese.getKey();
				if(sctxid==-1)
					continue;

				ctx = scp.contextMaps.getContextById(sctxid);
				Set<Integer> sourcesToShow = ctxsourcese.getValue();
				// the server sources may contain sources that are not
				// yet merged with the context
				Map<Integer, String> serverCtxSources = scp.contextMaps.getSourceIdNameMapForContextId(sctxid);
				if(serverCtxSources==null)
					serverCtxSources = new TreeMap<Integer, String>();

				if(sourcesToShow.size()==0)
					sourcesToShow.addAll(serverCtxSources.keySet());

				scr.reply.append(sctxid.toString());
				scr.reply.append(" ");
				scr.reply.append(Integer.toString(sourcesToShow.size()));
				for(Iterator<Integer> stsi = sourcesToShow.iterator(); stsi.hasNext(); ) {
					scr.reply.append(" ");
					Integer srcId = stsi.next();
					String srcName = scp.contextMaps.getSourceNameById(srcId);
					Map<Integer, String> prpIdPrpNameMap = scp.contextMaps.getPropertyIdNameMapForSourceId(srcId);
					if(prpIdPrpNameMap==null)
						prpIdPrpNameMap = new TreeMap<Integer, String>();

					scr.reply.append(srcId);

					scr.reply.append(" ");

					scr.reply.append(prpIdPrpNameMap.size());

					for(Iterator<Map.Entry<Integer, String>> pidpnameei = prpIdPrpNameMap.entrySet().iterator(); pidpnameei.hasNext(); ) {
						scr.reply.append(" ");

						Map.Entry<Integer, String> pidpnamee = pidpnameei.next();

						boolean isMerged = ctx.getSourceProperty(srcName, pidpnamee.getValue())!=null;

						scr.reply.append(pidpnamee.getKey().toString());
						if(!isMerged)
							scr.reply.append("*");
						scr.reply.append("=");
						scr.reply.append(Util.urlencode(pidpnamee.getValue()));
					}
				}
				if(ctxsourcesei.hasNext())
					scr.reply.append(" ");
			}
		}
		return scr;
	}

}
