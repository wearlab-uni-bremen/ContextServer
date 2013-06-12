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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.tzi.context.common.Context;
import org.tzi.context.common.ContextListenerInterface;
import org.tzi.context.server.ClientProxyListener;
import org.tzi.context.server.ContextServer;


public class CmdSubscribe implements ServerCommand {

	//@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		Integer ctxId = null;
		Context ctx = null;
		scr.usage = "Usage: SUBSCRIBE <#Contexts> (<ctxId> <#Sources> (<srcId> <#Props> (<propId> <#Tags> (<tag>){#Tags}){#Props}){#Sources}){#Contexts}";
		if(scp.words.length<2) {
			scr.commandError = true;
			scr.errorMsg = scr.usage;
		} else {
			int state = 0;
			int wIndex = 1;
			int numContextst = 0, numSources = 0, numProps = 0, numTags = 0;
			ctxId = 0;
			Integer srcId = 0, prpId = 0;
			String ctxName = null, srcName = null, prpName = null;
			Set<String> tags = new TreeSet<String>();
			Map<String, Map<String, Set<String>>> listenerMap = new TreeMap<String, Map<String,Set<String>>>();
			String currentWord;
			Set<String> unknownContexts = new TreeSet<String>();
			Set<String> unknownSources = new TreeSet<String>();
			Set<String> unknownProperties = new TreeSet<String>();
			Set<Integer> listenerIds = new TreeSet<Integer>();
			
			boolean stillNeedCtx = false;
			boolean stillNeedSrc = false;

			while(wIndex<scp.words.length && !scr.commandError) {
				currentWord = scp.words[wIndex++];
				switch(state) {
				case 0:
					try {
						numContextst = Integer.parseInt(currentWord);
						if(numContextst<0)
							throw new NumberFormatException();
						state = 1; // read context
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = scr.usage + "; error numctx";
					}
					break;
				case 1:
					ctx = null;
					if(numContextst>0) {
						ctxId = null;
						srcId = null;
						if("S".equals(currentWord)) {
							stillNeedCtx = true;
							numSources = 1;
							state = 3;
							numContextst--;
							break;
						}
						if("P".equals(currentWord)) {
							stillNeedCtx = true;
							stillNeedSrc = true;
							numSources = 1;
							numProps = 1;
							state = 5;
							numContextst--;
							break;
						}
						stillNeedCtx = false;
						ctxId = scp.csi.getCtxId(currentWord);
						ctxName = scp.csi.getStringId(currentWord);
						if(ctxId==null && ctxName==null) {
							scr.commandError = true;
							scr.errorMsg = "Unknown Context " + currentWord;
							continue;
						}
						if(ctxId==null) {
							unknownContexts.add(ctxName);
						} else {
							ctx = scp.contextMaps.getContextById(ctxId);
							
							if(ctxId==-1) {
								ctxName = Context.ALL_CONTEXTS; 
							} else {
								ctxName = ctx.getName();
							}
						}
						numContextst--;
						state = 2; // read sources count
					} else {
						state = -1; // go to default, skip words
					}
					break;
				case 2:
					try {
						numSources = Integer.parseInt(currentWord);
						if(numSources<0)
							throw new NumberFormatException();
						listenerMap.clear();
						state = 3; // read source
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = scr.usage + "; error numsrc";
					}
					break;
				case 3:
					if(numSources>0) {
						srcId = scp.csi.getSrcId(ctxId, currentWord);
						
						if(stillNeedCtx && srcId != null) {
							ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
							ctx = scp.contextMaps.getContextById(ctxId);
							ctxName = ctx.getName();
							stillNeedCtx = false;
						}
						
						srcName = scp.csi.getStringId(currentWord);
						if(srcId==null && srcName==null) {
							scr.commandError = true;
							scr.errorMsg = "Unknown Source " + currentWord;
							continue;
						}
						if(srcId==null) {
							unknownSources.add(ctxName);
							unknownSources.add(srcName);
						} else {
							// fallback when using wrong context-name
							// but numeric sourceId
							if(ctxId==null && srcId != -1) {
								ctxId = scp.contextMaps.getContextIdForSourceId(srcId);

								assert(ctxId!=null); // if there is a registered source...

								ctx = scp.contextMaps.getContextById(ctxId);
								ctxName = ctx.getName();
							}

							if(srcId==-1) {
								srcName = Context.ALL_SOURCES;
							} else {
								// only fetch name if context is known.
								// otherwise use given name...
								if(ctxId!=null)
									srcName = scp.contextMaps.getSourceNameById(srcId);
							}
						}

						state = 4; // read property count
						numSources--;
					} else {
						state = 1; // read next context
						ContextListenerInterface clp = 
							Context.createCLP(listenerMap);
						ClientProxyListener cpl = new ClientProxyListener(scp.csi, -1, clp, scp.cr);
						// add listener to control structures
						listenerIds.add(scp.csm.addClientProxyListener(scp.cr.getId(), cpl));
						if(ctx!=null) {
							ctx.addContextListener(cpl);
						} else {
							List<ClientProxyListener> cll = scp.contextMaps.pendingContextListener.get(ctxName);
							if(cll==null) {
								cll = new Vector<ClientProxyListener>();
								scp.contextMaps.pendingContextListener.put(ctxName, cll);
							}
							cll.add(cpl);
						}
						listenerMap.clear();
					}
					break;
				case 4:
					try {
						numProps = Integer.parseInt(currentWord);
						if(numProps<0)
							throw new NumberFormatException();
						state = 5; // read property
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = scr.usage + "; error numprop";
					}
					break;
				case 5:
					if(numProps>0) {
						prpId = scp.csi.getPrpId(srcId, currentWord);
						
						if(stillNeedSrc && prpId!=null) {
							srcId = scp.contextMaps.getSourceIdForPropertyId(prpId);
							srcName = scp.contextMaps.getSourceNameById(srcId);
							stillNeedSrc = false;
						}
						if(stillNeedCtx && srcId!=null) {
							ctxId = scp.contextMaps.getContextIdForSourceId(srcId);
							ctx = scp.contextMaps.getContextById(ctxId);
							ctxName = ctx.getName();
							stillNeedCtx = false;
						}
						
						prpName = scp.csi.getStringId(currentWord);
						if(prpId==null && prpName==null) {
							scr.commandError = true;
							scr.errorMsg = "Unknown Property " + currentWord;
							continue;
						}
						if(prpId==null) {
							unknownProperties.add(ctxName);
							unknownProperties.add(srcName);
							unknownProperties.add(prpName);
						} else {
							if(prpId != -1) {
								// infer other information from
								// numeric property name
								boolean hadNoSource = srcId == null;
								if(srcId == null) {
									srcId = scp.contextMaps.getSourceIdForPropertyId(prpId);
								}
								// fetch missing context only if real source is known
								if(ctxId == null) {
									ctxId = scp.contextMaps.getContextIdForSourceId(srcId);

									// must be valid now
									ctx = scp.contextMaps.getContextById(ctxId);
									ctxName = ctx.getName();
								}
								if(hadNoSource && ctxId != null) {
									srcName = scp.contextMaps.getSourceNameById(srcId);
								}
							}

							if(prpId==-1) {
								prpName = Context.ALL_PROPERTIES;
							} else {
								// valid now
								prpName = scp.contextMaps.getPropertyNameById(prpId);
							}
						}
						state = 6; // read tag count
						numProps--;
					} else {
						state = 3; // read next source
					}
					break;
				case 6:
					try {
						numTags = Integer.parseInt(currentWord);
						if(numTags<0)
							throw new NumberFormatException();
						tags.clear();
						if(numTags==0) {
							tags.add(Context.ALL_TAGS);

							Map<String, Set<String>> propMap = listenerMap.get(srcName);
							if(propMap==null) {
								propMap = new TreeMap<String, Set<String>>();
								listenerMap.put(srcName, propMap);
							}

							propMap.put(prpName, new TreeSet<String>(tags));

							state = 5;
						} else {
							state = 7; // read tags
						}
					} catch(NumberFormatException nfe) {
						scr.commandError = true;
						scr.errorMsg = scr.usage + "; error numtags";
					}
					break;
				case 7:
					if(numTags>0) {
						tags.add(currentWord);
						numTags--;

						if(numTags==0) {
							Map<String, Set<String>> propMap = listenerMap.get(srcName);
							if(propMap==null) {
								propMap = new TreeMap<String, Set<String>>();
								listenerMap.put(srcName, propMap);
							}

							propMap.put(prpName, new TreeSet<String>(tags));

							state = 5; // read next property
						}
					} else {
						state = 5;
					}
					break;
				default:
					// just skip words, used after parsing complete
				}
			}
			// can stop at -1, or at five (reading property without tags)
			if(state!=-1 && state!=5) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
			}

			if(!scr.commandError) {
				// the last element was not processed in the while loop
				if(listenerMap.size()>0) {
					ContextListenerInterface clp = 
						Context.createCLP(listenerMap);
					ClientProxyListener cpl = new ClientProxyListener(scp.csi, -1, clp, scp.cr);
					listenerIds.add(scp.csm.addClientProxyListener(scp.cr.getId(), cpl));
					// specific context
					if(ctx!=null) {
						if(ContextServer.debug)
							System.out.println("Attaching listener...");
						ctx.addContextListener(cpl);
					} else {
						if(ContextServer.debug)
							System.out.println("No context yet...");
						// if all contexts are specified, we need to add to pending and 
						// register with all existing contexts
						if(ctxId != null && ctxId == -1) {
							for(Context actx : scp.contextMaps.getContextMap().values()) {
								if(actx != null) { // there is a null-context...
									actx.addContextListener(cpl);
								}
							}
						} 

						List<ClientProxyListener> cll = scp.contextMaps.pendingContextListener.get(ctxName);
						if(cll==null) {
							cll = new Vector<ClientProxyListener>();
							scp.contextMaps.pendingContextListener.put(ctxName, cll);
						}
						cll.add(cpl);
					}
				}
			}
			if(!scr.commandError) {
				scr.reply.append(listenerIds.size());
				for(Integer lId : listenerIds) {
					scr.reply.append(" ");
					scr.reply.append(lId.toString());
				}
				scr.reply.append(" ");
				scr.reply.append(unknownContexts.size());
				for(String str : unknownContexts) {
					scr.reply.append(" ");
					scr.reply.append(str);
				}
				scr.reply.append(" ");
				scr.reply.append(unknownSources.size());
				for(String str : unknownSources) {
					scr.reply.append(" ");
					scr.reply.append(str);
				}
				scr.reply.append(" ");
				scr.reply.append(unknownProperties.size());
				for(String str : unknownProperties) {
					scr.reply.append(" ");
					scr.reply.append(str);
				}
			}
		}
		return scr;
	}

}
