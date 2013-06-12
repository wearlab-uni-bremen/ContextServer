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
package org.tzi.context.server;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import org.tzi.context.common.Context;
import org.tzi.context.common.ContextElement;
import org.tzi.context.common.ContextServerInterface;
import org.tzi.context.common.HistoryProvider;
import org.tzi.context.common.Protocol;
import org.tzi.context.common.Protocol.Command;
import org.tzi.context.common.Protocol.WriteMessageResult;
import org.tzi.context.common.Util;
import org.tzi.context.server.command.CmdCancelSubscription;
import org.tzi.context.server.command.CmdCreateContext;
import org.tzi.context.server.command.CmdCreateProperty;
import org.tzi.context.server.command.CmdCreateSource;
import org.tzi.context.server.command.CmdDeleteContext;
import org.tzi.context.server.command.CmdDeleteProperty;
import org.tzi.context.server.command.CmdDeleteSource;
import org.tzi.context.server.command.CmdGetContextID;
import org.tzi.context.server.command.CmdGetIDInfo;
import org.tzi.context.server.command.CmdGetProperty;
import org.tzi.context.server.command.CmdGetPropertyID;
import org.tzi.context.server.command.CmdGetSourceID;
import org.tzi.context.server.command.CmdHistory;
import org.tzi.context.server.command.CmdListClients;
import org.tzi.context.server.command.CmdListContexts;
import org.tzi.context.server.command.CmdListIds;
import org.tzi.context.server.command.CmdListProperties;
import org.tzi.context.server.command.CmdListSources;
import org.tzi.context.server.command.CmdListSubscriptions;
import org.tzi.context.server.command.CmdLogout;
import org.tzi.context.server.command.CmdPing;
import org.tzi.context.server.command.CmdPong;
import org.tzi.context.server.command.CmdSetProperty;
import org.tzi.context.server.command.CmdShortSubscription;
import org.tzi.context.server.command.CmdShutdown;
import org.tzi.context.server.command.CmdStartTime;
import org.tzi.context.server.command.CmdSubScript;
import org.tzi.context.server.command.CmdSubscribe;
import org.tzi.context.server.command.CmdTransferHandler;
import org.tzi.context.server.command.ServerCommand;
import org.tzi.context.server.command.ServerCommandParameters;
import org.tzi.context.server.command.ServerCommandResult;

public class ContextServer {
	
	public static final boolean debug = false;
	
	// property key to set scripting language
	public static final String propertyScriptLanguage = "org.tzi.context.scriptinglanguage";
	public static final String propertyScriptLanguageDefault = "ECMAScript";

	public static final String propertyHistory = "org.tzi.context.history";
	public static final String propertyHistoryDefault = "true";

	public static final String propertyPort = "org.tzi.context.port";
	public static final String propertyPortDefault = "" + Protocol.standardPort;
	
	// this is the core of the server, a single thread processing all clients sequentially
	public static class ServerThread extends Thread implements ContextServerInterface, ContextSubscriptionManager, HistoryProvider {
		
		private boolean scriptingAvailable = false;
		private ScriptEngineManager scriptEngineManager = null;
		private ScriptEngineFactory scriptEngineFactory = null;
		
		private boolean historyInit = true;
		private HistoryDBAbstraction hdb = null;
		
		// unique start time; serves as a marker to clients if reconnecting after server restart
		private long startTime = System.currentTimeMillis();
		
		public long getStartTime() {
			return startTime;
		}
		
		private void historyRemoveContext(String ctxName) {
			if(historyInit)
				return;
			
			hdb.removeContext(ctxName);
		}

		private void historyAddContext(String ctxName) {
			if(historyInit)
				return;
			
			hdb.addContext(ctxName);
		}
		
		private void historyAddSource(Integer ctxId, String srcName) {
			if(historyInit)
				return;
			
			Context ctx = contextMaps.getContextById(ctxId);
			if(ctx==null)
				return;

			hdb.addSource(ctx.getName(), srcName);
		}
		
		private void historyRemoveSource(Integer srcId) {
			if(historyInit)
				return;
			
			Integer ctxId = contextMaps.getContextIdForSourceId(srcId);
			
			if(ctxId == null)
				return;

			Context ctx = contextMaps.getContextById(ctxId);
			
			if(ctx == null)
				return;
			
			String srcName = contextMaps.getSourceNameById(srcId);
			
			if(srcName == null)
				return;
			
			hdb.removeSource(ctx.getName(), srcName);
		}		
		
		
		private void historyAddProperty(Integer serverSrcId, String prpName) {
			if(historyInit)
				return;

			Integer serverCtxId = contextMaps.getContextIdForSourceId(serverSrcId);
			Context ctx = contextMaps.getContextById(serverCtxId);
			String srcName = contextMaps.getSourceNameById(serverSrcId);
			
			hdb.addProperty(ctx.getName(), srcName, prpName);
		}
		
		private void historyRemoveProperty(Integer serverPrpId) {
			if(historyInit)
				return;
			
			Integer serverSrcId = contextMaps.getSourceIdForPropertyId(serverPrpId);
			Integer serverCtxId = contextMaps.getContextIdForSourceId(serverSrcId);
			
			String ctxName = contextMaps.getContextById(serverCtxId).getName();
			String srcName = contextMaps.getSourceNameById(serverSrcId);
			String prpName = contextMaps.getPropertyNameById(serverPrpId);
			
			hdb.removeProperty(ctxName, srcName, prpName);
		}
		
		private void setupFromHistory() {
			for(String context : hdb.getActiveContexts()) {
				Integer ctxId = addContext(context);
				
				for(String source : hdb.getActiveSources(context)) {
					Integer srcId = addSource(ctxId, source);

					for(String property : hdb.getActiveProperties(context, source)) {
						addProperty(srcId, property);
						
						ContextElement ce = hdb.getLastState(context, source, property);
						if(ce!=null) {
							contextMaps.getContextById(ctxId).mergeContextElement(ce);
						}
					}
				}
			}
			
			historyInit = false;
		}
		
		private void initHistoryDB(Properties props) {
			boolean useHistory = Boolean.parseBoolean(props.getProperty(propertyHistory, propertyHistoryDefault));
			
			if(useHistory) {
				System.out.println("Preparing history functionality...");
				
				String histDBProps = props.getProperty("org.tzi.context.historyDBProps");
				
				// load defaults
				DefaultDBProperties dbProps = new DefaultDBProperties();
				// load modifiers from file
				if(histDBProps!=null) {
					InputStream histDBPropsIS = ClassLoader.getSystemClassLoader().getResourceAsStream(histDBProps);
					try {
						dbProps.load(histDBPropsIS);
						histDBPropsIS.close();
					} catch (Exception e1) {
						throw new RuntimeException("Error processing history properties!");
					}
				}

				// supersede with given arguments
				dbProps.putAll(props);
				
				String historyClass = dbProps.getProperty(DefaultDBProperties.KEY_CLASS);
				
				if(historyClass==null) {
					System.err.println("No class for history object given... terminating!");
					System.exit(1);
				}
				
				try {
					Class<?> c = Class.forName(historyClass);
					
					boolean isHist = false;
					
					Class<?> sups = c;
					
					while(sups != null && sups != HistoryDBAbstraction.class) {
						sups = sups.getSuperclass();
					}
					if(sups == HistoryDBAbstraction.class) {
						isHist = true;
					}

					if(!isHist) {
						System.err.println("Given class for history is not a HistoryAbstraction... terminating!");
						System.exit(1);
					}

					Constructor<?> con = c.getDeclaredConstructor(Properties.class);
					Object o = con.newInstance(dbProps);

					hdb = (HistoryDBAbstraction)o;

					if(o instanceof HistoryDB) {
						HistoryDB hdbo = (HistoryDB)o;

						String classDriver = hdb.getProperty(DefaultDBProperties.KEY_DRIVER);

						if(classDriver!=null) {
							System.out.println("Trying to initialize Database-Driver-Class: " + classDriver);
							try {
								Class.forName(classDriver);
							} catch (ClassNotFoundException e) {
								System.err.println("Initialization failed...");
								e.printStackTrace();
							}
						}

						String histDB = hdb.getProperty(DefaultDBProperties.KEY_DBCONNECTION);

						if(histDB == null) {
							System.err.println("No Database for history specified!");
							return;
						} else {
							String connectionString = histDB;

							System.out.println("Connecting to history database: " + connectionString);

							if(hdbo.connectToDatabase()) {
								hdb.initialize();
								setupFromHistory();

							} else {
								System.err.println("There was an error while connecting to the database...");
							}
						}
					} else {
						hdb.initialize();
						setupFromHistory();
					}
					
					if(debug) {
						System.out.println("ActiveContexts: " + hdb.getActiveContexts());
						for(String ctx : hdb.getActiveContexts()) {
							System.out.println("Active sources in " + ctx);
							for(String src : hdb.getActiveSources(ctx)) {
								System.out.println("  " + src + ": " + hdb.getActiveProperties(ctx, src));
							}
						}
					}
				} catch (Exception e1) {
					System.err.format("Unable to load history class '%s'... terminating!\n", historyClass);
					System.exit(1);
				}
			}
		}
		
		public boolean scriptingAvailable() {
			return scriptingAvailable;
		}
		
		// some housekeeping variables
		private int serverPort = Protocol.standardPort;
		private ServerSocket ss = null;
		
		private Random rnd = new Random();
		
		// set of used ids; assigned to all entities
		private Set<Integer> usedIds = new TreeSet<Integer>();
		
		public Set<Integer> getUsedIds() {
			return usedIds;
		}

		// a storage object to keep everything that is client related in one spot
		// and to aid in readability
		private ClientMaps clientMaps = new ClientMaps();
		
		// a storage object to keep everything context/data related in one spot
		private ContextMaps contextMaps = new ContextMaps();
		
		// global flag for shutting down the server loop
		private boolean shutdown = false;
		
		public Integer getUniqueId() {
			// TODO: need to check why I choose to synchronize this. should actually not be necessary...
			synchronized (usedIds) {
				Integer id = -1;
				
				do {
					while(id<1) 
						id=Integer.valueOf(rnd.nextInt());
				} while(usedIds.contains(id));
				
				usedIds.add(id);
				return id;
			}
		}
		
		public void freeId(Integer uid) {
			synchronized (usedIds) {
				usedIds.remove(uid);
			}
		}
		
		public Integer getContextId(Context ctx)
		{
			// TODO: If threads use this, we might get a problem...
			// Currently everything is single-threaded.
			for(Map.Entry<Integer, Context> meic : contextMaps.getContextMap().entrySet())
			{
				if(meic.getValue() == ctx)
					return meic.getKey();
			}
			return null;
		}

		public Integer addClientProxyListener(Integer cId, ClientProxyListener cpl) {
			Integer lId = getUniqueId();
			cpl.setId(lId);
			
			Map<Integer, ClientProxyListener> cplMap = contextMaps.proxyListenerMap.get(cId);
			if(cplMap==null) {
				cplMap = new TreeMap<Integer, ClientProxyListener>();
				contextMaps.proxyListenerMap.put(cId, cplMap);
			}
			
			cplMap.put(lId, cpl);
			
			return lId;
		}
		
		private void addClient(ClientRepresentation cr) {
			Integer id = getUniqueId();
			cr.setId(id);
			clientMaps.idClientMap.put(id, cr);
		}
		
		private void removeClient(Integer cId) {
			removeAllClientSubscriptions(cId);
			clientMaps.idClientMap.remove(cId);
			freeId(cId);
		}
		
		public void removeClientSubscription(Integer cId, Integer sId) {
			Map<Integer, ClientProxyListener> lmap = contextMaps.proxyListenerMap.get(cId);
			ClientProxyListener cpl = lmap.get(sId);
			if(cpl!=null) {
				// we need to remove from all because
				// there might be ALL_CTX listeners
				for(Context ctx : contextMaps.getContextMap().values()) {
					if(ctx!=null) {
						ctx.removeContextListener(cpl);
					}
				}
				// finally remove pending subscriptions and subs. to ALL_CTX
				for(List<ClientProxyListener> cll : contextMaps.pendingContextListener.values()) {
					if(cll != null) {
						cll.remove(cpl);
					}
				}
			}
			lmap.remove(sId);
			freeId(sId);
		}
		
		public void removeAllClientSubscriptions(Integer cId) {
			Map<Integer, ClientProxyListener> lmap = contextMaps.proxyListenerMap.get(cId);
			if(lmap!=null) {
				Set<Integer> mapKeys = new TreeSet<Integer>(lmap.keySet());
				for(Integer sId : mapKeys) {
					removeClientSubscription(cId, sId);
				}
			}
			contextMaps.proxyListenerMap.remove(cId);
		}
		
		public void setShortStateClientSubscription(Integer cId, Integer sId, boolean shortFormat) {
			Map<Integer, ClientProxyListener> lmap = contextMaps.proxyListenerMap.get(cId);
			if(lmap==null)
				return;
			
			ClientProxyListener cpl = lmap.get(sId);
			
			if(cpl == null)
				return;
			
			cpl.setShortFormat(shortFormat);
		}
		
		public void setShortStateAllClientSubscription(Integer cId, boolean shortFormat) {
			Map<Integer, ClientProxyListener> lmap = contextMaps.proxyListenerMap.get(cId);
			if(lmap!=null) {
				for(ClientProxyListener cpl : lmap.values())
					cpl.setShortFormat(shortFormat);
			}
		}

		public boolean getShortStateClientSubscription(Integer cId, Integer sId) {
			Map<Integer, ClientProxyListener> lmap = contextMaps.proxyListenerMap.get(cId);

			if(lmap==null)
				return false;
			
			ClientProxyListener cpl = lmap.get(sId);

			if(cpl == null)
				return false;
			
			return cpl.isShortFormat();
		}
		
		// called by server, to register source
		public Integer addSource(int ctxId, String source) {
			Integer srcId = contextMaps.getSourceIdByName(ctxId, source);
				
			if(srcId!=null)
				return srcId;
			
			srcId = contextMaps.addSourceNameIdForContextId(ctxId, source);
			
			historyAddSource(ctxId, source);
			
			return srcId;
		}
		
		public void removeSource(int srcId) {
			
			Map<Integer, String> prpIdPrpNameMap = contextMaps.getPropertyIdNameMapForSourceId(srcId);
			if(prpIdPrpNameMap != null && prpIdPrpNameMap.size() > 0) {
				// use temporary list to avoid concurrent modification
				List<Integer> deleteKeys = new Vector<Integer>(prpIdPrpNameMap.keySet());
				for(Integer prpId : deleteKeys)
					removeProperty(prpId);
			}
			
			String srcName = contextMaps.getSourceNameById(srcId);
			
			contextMaps.removeSource(srcId);
			freeId(srcId);
			
			Context ctx = contextMaps.getContextForSourceId(srcId);
			ctx.removeSource(srcName);
			
			historyRemoveSource(srcId);
		}
		
		public Integer addProperty(int srcId, String property) {
			Integer prpId = contextMaps.getPropertyIdByName(srcId, property);
			if(prpId!=null)
				return prpId;
			
			prpId = contextMaps.addPropertyNameIdForSourceId(srcId, property);
			
			historyAddProperty(srcId, property);
			
			return prpId;
		}
		
		// remove property from control structures and clean up if needed
		public void removeProperty(int prpId) {
			String prpName = contextMaps.getPropertyNameById(prpId);
			contextMaps.removeProperty(prpId);
			freeId(prpId);
			
			Integer ctxId = contextMaps.getContextIdForPropertyId(prpId);
			Context ctx = contextMaps.getContextById(ctxId);
			ctx.removeSourceProperty(contextMaps.getSourceNameForPropertyId(prpId), prpName);
			
			historyRemoveProperty(prpId);
		}
		
		public Integer addContext(String ctxName) {
			Integer ctxId = contextMaps.getContextIdByName(ctxName);
			
			if(ctxId!=null)
				return ctxId;
			
			Context ctx = new Context(ctxName);
			ctxId = contextMaps.addContext(ctx);
			
			historyAddContext(ctxName);
			
			return ctxId;
		}
		
		public void removeContext(int ctxId) {
			Context ctx = contextMaps.getContextById(ctxId);
			
			Map<Integer, String> srcIdSrcNameMap = contextMaps.getSourceIdNameMapForContextId(ctxId);
			
			if(srcIdSrcNameMap != null) {
				Set<Integer> remset = new TreeSet<Integer>();
				remset.addAll(srcIdSrcNameMap.keySet());
				
				for(Integer srcId : remset) {
					String srcName = srcIdSrcNameMap.get(srcId);
					removeSource(srcId);
					ctx.removeSource(srcName);
				}
			}
			
			contextMaps.removeContext(ctxId);
			
			freeId(ctxId);
			
			historyRemoveContext(ctx.getName());
		}
		
		public void propertyChange(int prpId, ContextElement ce) {
			if(historyInit)
				return;
			
			Context ctx = contextMaps.getContextForPropertyId(prpId);
			
			if(ctx == null)
				return;
			
			String srcName = contextMaps.getSourceNameForPropertyId(prpId);
			String prpName = contextMaps.getPropertyNameById(prpId);
			
			hdb.propertyChange(ctx.getName(), srcName, prpName, ce.getTimestamp(), ce.getValue(), ce.getTypeTags(), ce.isPersistent());
		}
		
		private class ListenerThread extends Thread {
			public ListenerThread() {
				setDaemon(true);
			}
			
			public void run() {
				if(debug)
					System.out.println("Listener started...");
				while(!shutdown) {
					try {
						Socket s = ss.accept();
						s.setSoTimeout(10);
						synchronized(clientMaps.newConnections) {
							clientMaps.newConnections.offer(s);
						}
					}
					// SocketException is thrown when closing ss
					catch(SocketException te) {
						if(!shutdown) {
							System.err.println(this.getClass().getCanonicalName());
							te.printStackTrace();
						}
					}
					catch (IOException e) {
						System.err.println(this.getClass().getCanonicalName());
						e.printStackTrace();
					}
				}
			}
		}
		
		public ServerThread(Properties props) throws IOException {
			String portS = props.getProperty(propertyPort, propertyPortDefault);
			int port = Integer.parseInt(portS);
			
			String scriptLanguage = props.getProperty(propertyScriptLanguage, propertyScriptLanguageDefault);
			
			scriptEngineManager = new ScriptEngineManager();
			scriptingAvailable = false;
			if(!scriptLanguage.equalsIgnoreCase("none")) {
				for(ScriptEngineFactory sef : scriptEngineManager.getEngineFactories()) {
					if(sef.getLanguageName().equals(scriptLanguage)) {
						scriptingAvailable = true;
					}
					if(!scriptingAvailable) {
						for(String shortName : sef.getNames()) {
							if(shortName.equals(scriptLanguage)) {
								scriptingAvailable = true;
								break;
							}
						}
					}

					if(scriptingAvailable) {
						ScriptEngine engine = sef.getScriptEngine();
						if(engine instanceof Invocable) {
							System.out.println("Using " + sef.getEngineName() + " for scripting.");
							scriptEngineFactory = sef;
							break;
						} else {
							System.out.println("Script Engine " + sef.getEngineName() + " is not invocable!");
							scriptingAvailable = false;
						}
					}
				}
			}
			
			if(!scriptingAvailable) {
				scriptEngineManager = null;
				System.out.println("Scripting not available!");
			}
			
			serverPort = port;
			ss = new ServerSocket(serverPort);
			ss.setReuseAddress(true);
			
			initHistoryDB(props);
			
			ListenerThread lt = new ListenerThread();
			lt.start();
		}
		
		
		public String getStringId(String idS) {
			if(idS.startsWith("@"))
				return idS.substring(1);
			
			return null;
		}
		
		public Integer getCtxId(String idS) {
			Integer id;

			if(idS.equalsIgnoreCase(Context.ALL_CONTEXTS))
				return -1;

			if(idS.startsWith("@")) {
				id = contextMaps.getContextIdByName(idS.substring(1));
			} else {
				try {
					id = Integer.parseInt(idS);
					if(contextMaps.getContextById(id)==null)
						id = null;
				} catch(NumberFormatException nfe) {
					id = null;
				}
			}
			
			return id;
		}
		
		public Integer getSrcId(Integer ctxId, String idS) {
			Integer id;

			if(idS.equalsIgnoreCase(Context.ALL_SOURCES))
				return -1;

			if(ctxId!=null && idS.startsWith("@")) {
				Map<String, Integer> srcNameSrcIdMap = contextMaps.getSourceNameIdMapForContextId(ctxId);
				if(srcNameSrcIdMap==null) {
					id = null;
				} else {
					id = srcNameSrcIdMap.get(idS.substring(1));
				}
			} else {
				try {
					id = Integer.parseInt(idS);
					if(contextMaps.getContextIdForSourceId(id)==null)
						id = null;
				} catch(NumberFormatException nfe) {
					id = null;
				}
			}
			
			return id;
		}
		
		public Integer getPrpId(Integer srcId, String idS) {
			Integer id;
			
			if(idS.equalsIgnoreCase(Context.ALL_PROPERTIES))
				return -1;
			
			if(srcId!=null && idS.startsWith("@")) {
				Map<String, Integer> prpNamePrpIdMap = contextMaps.getPropertyNameIdMapForSourceId(srcId);
				if(prpNamePrpIdMap==null) {
					id = null;
				} else {
					id = prpNamePrpIdMap.get(idS.substring(1));
				}
			} else {
				try {
					id = Integer.parseInt(idS);
					if(contextMaps.getSourceIdForPropertyId(id)==null)
						id = null;
				} catch(NumberFormatException nfe) {
					id = null;
				}
			}
			
			return id;
		}

		public void run() {
			ServerCommandParameters scp = new ServerCommandParameters();
			scp.csi = this;
			scp.csm = this;
			scp.sem = scriptEngineManager;
			scp.sef = scriptEngineFactory;
			scp.clientMaps = clientMaps;
			scp.contextMaps = contextMaps;
			scp.history = this;
			
			/* All context-related commands have been refactored out of the
			 * server code into separate classes.
			 * They are mapped by their corresponding commands.
			 */
			Map<Command, ServerCommand> commandMap = new TreeMap<Command, ServerCommand>();
			// List clients
			commandMap.put(Command.LISTCLT, new CmdListClients());
			// List contexts
			commandMap.put(Command.LISTCTX, new CmdListContexts());
			// Create a new context
			commandMap.put(Command.CREATECTX, new CmdCreateContext());
			// List sources (for a context)
			commandMap.put(Command.LISTSRC, new CmdListSources());
			// List properties for a context (and selected sources)
			commandMap.put(Command.LISTPRP, new CmdListProperties());
			// Create a new source
			commandMap.put(Command.CREATESRC, new CmdCreateSource());
			// Remove a source
			commandMap.put(Command.DELETEPRP, new CmdDeleteProperty());
			// Delete a context
			commandMap.put(Command.DELETESRC, new CmdDeleteSource());
			// Delete a context
			commandMap.put(Command.DELETECTX, new CmdDeleteContext());
			// Create a property for a source
			commandMap.put(Command.CREATEPRP, new CmdCreateProperty());
			// Set a property
			commandMap.put(Command.SETPRP, new CmdSetProperty());
			// Get a property (value)
			commandMap.put(Command.GETPRP, new CmdGetProperty());
			// Get a context ID (from name)
			commandMap.put(Command.GETCTXID, new CmdGetContextID());
			// Get a property ID (from name)
			commandMap.put(Command.GETPRPID, new CmdGetPropertyID());
			// Get a source ID (from name)
			commandMap.put(Command.GETSRCID, new CmdGetSourceID());
			// Subscribe to context-events
			commandMap.put(Command.SUBSCRIBE, new CmdSubscribe());
			// Subscribe to context-events via script
			commandMap.put(Command.SUBSCRIPT, new CmdSubScript());
			// List subscriptions
			commandMap.put(Command.LISTSUB, new CmdListSubscriptions());
			// Cancel a subscription
			commandMap.put(Command.CANCELSUB, new CmdCancelSubscription());
			// Set short format for a subscription
			commandMap.put(Command.SHORTSUB, new CmdShortSubscription());
			// List used ids (debugging)
			commandMap.put(Command.LISTIDS, new CmdListIds());
			// Keepalive: react to a ping (send pong)
			commandMap.put(Command.PING, new CmdPing());
			// Keepalive: react to a pong (do nothing)
			commandMap.put(Command.PONG, new CmdPong());
			// Logout request (not context related)
			commandMap.put(Command.LOGOUT, new CmdLogout());
			// History Query
			commandMap.put(Command.HISTORY, new CmdHistory());
			// Transfers
			CmdTransferHandler transferHandler = new CmdTransferHandler();
			commandMap.put(Command.TXPACKET, transferHandler);
			commandMap.put(Command.TXACK, transferHandler);
			commandMap.put(Command.TXCANCEL, transferHandler);
			commandMap.put(Command.TXRESEND, transferHandler);
			commandMap.put(Command.STARTTIME, new CmdStartTime());
			commandMap.put(Command.GETIDINFO, new CmdGetIDInfo());
			commandMap.put(Command.SHUTDOWN, new CmdShutdown());
			
			byte [] buffer = new byte [Protocol.maxDataSize];

			System.out.println("Server started...");
			int idle_count = 0;
			int cycle_count = 0;
			while(!shutdown) {
				if(cycle_count >= 1000) {
					//System.out.format("Was idle %d of %d cycles...\n", idle_count, cycle_count);
					idle_count = 0 * idle_count;
					cycle_count = 0;
				}
				
				boolean idle = true;
				// Check new clients for LOGIN or RELOGIN
				for(Iterator<Socket> si = clientMaps.pendingConnections.iterator(); si.hasNext(); ) {
					Socket s = si.next();
					if(s==null) {
						si.remove();
						continue;
					}
					try {
						int lbindex = 0;
						InputStream is = s.getInputStream();
						OutputStream os = s.getOutputStream();
						String name;
						ClientRepresentation cr;
						while(is.available()>0 && lbindex<Protocol.maxDataSize) {
							int r = is.read(buffer, lbindex, Protocol.maxDataSize-lbindex);
							lbindex += r;
						}
						if(lbindex>0) {
							long currentTime = System.currentTimeMillis();
							String message = Protocol.decodeString(buffer, 0, lbindex).trim();
							String [] words = Util.splitWS(message);
							String commandS = words[0];
							Command command = Protocol.getCommand(commandS);
							if(debug)
								System.out.println("Got: \"" + message + "\", cmd = " + command);
							switch(command) {
							case LOGIN:
								if(words.length<2) {
									Protocol.writeMessage(os, Protocol.FAIL);
									s.close();
								} else {
									name = Util.urldecode(words[1]);
									cr = new ClientRepresentation(name);
									addClient(cr);
									Protocol.writeMessage(os, Protocol.ACCEPT + " " + name + " " + cr.getId());

									clientMaps.clientMap.put(s, cr);
									cr.setLastMessageTime(currentTime);
									cr.setLastSendTime(currentTime);
									cr.setLastPingTime(currentTime);
								}
								si.remove();
								break;
							case RELOGIN:
								boolean reloginFailed = false;
								String idS = null;
								name = null;

								// RELOGIN <name [part [part]]> <id>
								if(words.length<3) {
									cr = null;
									reloginFailed = true;
								} else {

									idS = words[2];
									name = Util.urldecode(words[1]);

									int re_id;
									try {
										re_id = Integer.parseInt(idS);
									}
									catch(NumberFormatException nfe) {
										re_id = -1;
										reloginFailed = true;
									}

									cr = clientMaps.idClientMap.get(Integer.valueOf(re_id));
								}
								
								if(cr!=null) {
									if(cr.getName().equals(name)) {
										for(Iterator<Map.Entry<Socket, ClientRepresentation>> screi = clientMaps.clientMap.entrySet().iterator(); screi.hasNext();) {
											Map.Entry<Socket, ClientRepresentation> scre = screi.next();
											if(scre.getValue() == cr) {
												screi.remove();
												scre.getKey().close();
												break;
											}
										}
										// remove client from the potential kick list
										clientMaps.limboMap.remove(cr.getId());
										clientMaps.clientMap.put(s, cr);
										Protocol.writeMessage(os, Protocol.ACCEPT + " " + name + " " + cr.getId());
										cr.setLastMessageTime(currentTime);
										cr.setLastSendTime(currentTime);
										cr.setLastPingTime(currentTime);

										si.remove();
									} else {
										reloginFailed = true;
									}
								} else {
									reloginFailed = true;
								}
								if(reloginFailed) {
									Protocol.writeMessage(os, Protocol.FAIL);
									si.remove();
									s.close();
								}
								break;
							default:
								Protocol.writeMessage(os, Protocol.DROP);
								si.remove();
								s.close();
							}
						}
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} // END check new clients
				
				// check for new connections accepted by the listener thread
				synchronized(clientMaps.newConnections) {
					while(!clientMaps.newConnections.isEmpty()) {
						Socket s = clientMaps.newConnections.poll();
						if(s!=null) {
							try {
								Protocol.writeMessage(s.getOutputStream(), Protocol.HELLO);
								clientMaps.pendingConnections.offer(s);
							} catch (IOException e) {
							}
						}
					}
				}
				
				// process clients
				for(Iterator<Map.Entry<Socket, ClientRepresentation>> screi = clientMaps.clientMap.entrySet().iterator(); screi.hasNext();) {
					Map.Entry<Socket, ClientRepresentation> scre = screi.next();
					ClientRepresentation cr = scre.getValue();

					scp.cr = cr;
					
					long currentTime = System.currentTimeMillis();
					long idleTime = currentTime - cr.getLastMessageTime();
					long betweenSendTime = currentTime - cr.getLastSendTime();
					long betweenPingTime = currentTime - cr.getLastPingTime();
					
					boolean removeClient = false;
					
					try {
						InputStream is = scre.getKey().getInputStream();
						OutputStream os = scre.getKey().getOutputStream();
						
						/* 
						 * Check for timeout and put on limbo-list if timeout is reached
						 */
						if(idleTime > Protocol.timeout) {
							screi.remove();
							clientMaps.limboMap.put(cr.getId(), currentTime);
							if(debug)
								System.out.println("Timeout; Dropping " + cr.getName() + " #" + cr.getId());
							try {
								Protocol.writeMessage(os, Protocol.DROP);
								scre.getKey().close();
							} catch(IOException ioe) {
								if(debug)
									System.out.println("Error disconnecting client; assuming connection loss...");
							}
							continue; // next client
						}
						
						/*
						 * Check for Keepalive need
						 */
						if(betweenSendTime > Protocol.heartbeat || (betweenPingTime > Protocol.heartbeat && idleTime > Protocol.heartbeat) ) {
							Protocol.writeMessage(os, Protocol.PING);
							cr.setLastSendTime(currentTime);
							cr.setLastPingTime(currentTime);
						}
						
						/*
						 * Process messages
						 */
						String message = null;
						// process old messages...
						if(!cr.getIncomingMessages().isEmpty()) {
							message = cr.getIncomingMessages().poll();
						// ...or check for new messages
						} else {
							while(is.available()>0 && cr.bindex<Protocol.maxDataSize) {
								int r = is.read(cr.buffer, cr.bindex, Protocol.maxDataSize-cr.bindex);
								cr.bindex += r;
							}
							if(cr.bindex>0) {
								if(!Protocol.endsWithNewline(cr.buffer, cr.bindex)) {
									int lastNewLineIndex = Protocol.lastNewLine(cr.buffer, cr.bindex);
									if(lastNewLineIndex>0) {
										int size = lastNewLineIndex + Protocol.newLineBytes.length;
										message = Protocol.decodeString(cr.buffer, 0, size);
										int remaining = cr.bindex - size;
										//System.out.format("Data is %s (%d bytes), remaining %d...\n", message.trim(), size, remaining);
										for(int i=0; i<remaining; i++) {
											cr.buffer[i] = cr.buffer[i+size];
										}
										cr.bindex -= size;
									} else {
										System.err.println("Invalid transmission. New newline found!");
										cr.bindex = 0;
									}
								} else {
									message = Protocol.decodeString(cr.buffer, 0, cr.bindex).trim();
									cr.bindex = 0;
								}
								if(message!=null) {
									// if the client is fast, we might
									// get more than one message...
									String [] moremsg = Util.splitNL(message);
									if(moremsg.length>1) {
										message = moremsg[0];
										for(int i=1; i<moremsg.length; i++) {
											cr.getIncomingMessages().offer(moremsg[i]);
										}
									}
								}
							}
						}
						
						// If there is a message to process:
						if(message!=null) {
							idle = false;
							String [] words = Util.splitWS(message);
							String prefix = "";
							
							if(words.length>1 && words[0].charAt(0) == Protocol.PREFIX_CHAR) {
								prefix = words[0] + " ";
								
								String [] tw = words;
								words = new String [words.length-1];
								
								for(int i=1; i<tw.length; i++)
									words[i-1] = tw[i];
							}
							
							String commandS = words[0];
							Command command = Protocol.getCommand(commandS);
							
							scp.commandS = commandS;
							scp.command = command;
							
							//if(debug)
								//System.out.println("Message from " + cr.getName() + ": \"" + message + "\", cmd = " + command);
							cr.setLastMessageTime(System.currentTimeMillis());
							
							ServerCommandResult scr = null;
							
							// get command from map
							ServerCommand sc = commandMap.get(scp.command);
							
							if(sc!=null) {
								// TX commands may produce invalid 
								// Strings for urldecode and therefore
								// expect (need) unmodified words
								if(!Protocol.TXPACKET.equals(words[0])) {
									for(int i=0; i<words.length; i++)
										words[i] = Util.urldecode(words[i]);
								}
								scp.words = words;

								// this is much easier now...
								try {
									scr = sc.processCommand(scp);
								} catch(RuntimeException e) {
									System.err.println("Exception while processing command...");
									e.printStackTrace();
									scr = new ServerCommandResult(null, prefix + "Internal server error!", true, false);
								}
							} else {
								if(debug)
									System.out.println("No mapped command for " + command + ": " + scp.commandS);
								scr = new ServerCommandResult(null, prefix + "Invalid command", true, false);
							}
							
							// special handling for logout
							if(scp.command == Command.LOGOUT)
							{
								removeClient = true;
							}
							
							// check type of result
							if(scr.commandError) {
								Protocol.writeMessage(os, prefix + Protocol.FAIL + " " + scr.errorMsg);
							} else {
								if(scr.writeOK) {
									Protocol.writeMessage(os, prefix + Protocol.OK);
								} else {
									if(scr.reply.length()>0) {
										WriteMessageResult res = Protocol.writeMessage(os, prefix + scr.reply.toString(), false, this);
										if(res.isTransfer()) {
											cr.startTransferToClient(res.getTransferId(), res.getPacketBytes());
										}
									}
								}
								
								if(sc instanceof CmdShutdown && !scr.commandError) {
									System.out.println("Server shutting down...");
									shutdown = true;
								}
							}
							
							// almost always true
							//cr.setLastSendTime(currentTime);
						}
						
						/*
						 * After handling the protocol we need to check if
						 * the proxy-listeners have added new (context-)messages
						 */
						synchronized (cr.getMessageQueue()) {
							long mstart = System.currentTimeMillis();
							int mcount = 0;
							Queue<String> msgQueue = cr.getMessageQueue();
							//if(debug)
								//if(!msgQueue.isEmpty())
									//System.out.println("Sending Messages");
							while(!msgQueue.isEmpty()) {
								String msg = msgQueue.poll();
								if(msg!=null) {
									idle = false;
									WriteMessageResult res = Protocol.writeMessage(os, msg, true, this);
									if(res.isTransfer()) {
										cr.startTransferToClient(res.getTransferId(), res.getPacketBytes());
									}
								}
								mcount++;
								if(mcount == 16) {
									mcount = 0;
									if((System.currentTimeMillis()-mstart) > 40) {
										if(debug)
											System.out.println("...leaving messages...");
									}
									break;
								}
							}
						}
						
						if(!idle) {
							cr.setLastSendTime(System.currentTimeMillis());
						}
						
					// if a client dropped, put on limbo map for reconnect
					} catch(IOException ioe) {
						clientMaps.limboMap.put(cr.getId(), currentTime);
						if(debug)
							System.out.println("IOError; Dropping " + cr.getName() + " #" + cr.getId() + ": " + ioe.getMessage());
						screi.remove();
						
						// if there was an error while logging out, do not perform (clean-)logout
						removeClient = false;
					}
					
					// Finalize logout
					if(removeClient) {
						removeClient(cr.getId());
						screi.remove();
						try {
							scre.getKey().close();
						} catch(IOException ioe) {
							// so what ?
						}
						clientMaps.clientMap.remove(scre.getKey());
					}
				}
				
				/*
				 * Check limbo-map for final timeouts 
				 */
				Long currentTime = System.currentTimeMillis();
				for(Iterator<Map.Entry<Integer, Long>> lmei = clientMaps.limboMap.entrySet().iterator(); lmei.hasNext(); ) {
					Map.Entry<Integer, Long> lme = lmei.next();
					if( (currentTime - lme.getValue()) > Protocol.limboTime ) {
						// time to kick ass and chew bubblegum...
						removeClient(lme.getKey());
						if(debug)
							System.out.println("Final removement of Client #" + lme.getKey());
						lmei.remove();
					}
				}
				
				cycle_count++;
				if(idle)
					idle_count++;
				
				try {
					sleep(0, 500);
				} catch (InterruptedException e) {
				}
			}

			for(Map.Entry<Socket, ClientRepresentation> scre : clientMaps.clientMap.entrySet()) {
				try {
					OutputStream os = scre.getKey().getOutputStream();
					
					Protocol.writeMessage(os, Protocol.DROP);
					
					os.flush();
					os.close();
					scre.getKey().close();
				} catch (IOException e) {
				}
			}
			
			System.out.println("Server exited!");
		}

		@Override
		public long earliestRecord(Integer serverPrpId) {
			if(historyInit)
				return -1;
			
			Integer serverSrcId = contextMaps.getSourceIdForPropertyId(serverPrpId);
			Integer serverCtxId = contextMaps.getContextIdForSourceId(serverSrcId);
			
			String ctxName = contextMaps.getContextById(serverCtxId).getName();
			String srcName = contextMaps.getSourceNameById(serverSrcId);
			String prpName = contextMaps.getPropertyNameById(serverPrpId);

			return hdb.getPrpTimestamp(ctxName, srcName, prpName, false);
		}

		@Override
		public List<ContextElement> getHistory(Integer serverPrpId, long from,
				long to, int limit, Set<String> withTags) {
			Integer serverSrcId = contextMaps.getSourceIdForPropertyId(serverPrpId);
			Integer serverCtxId = contextMaps.getContextIdForSourceId(serverSrcId);
			
			String ctxName = contextMaps.getContextById(serverCtxId).getName();
			String srcName = contextMaps.getSourceNameById(serverSrcId);
			String prpName = contextMaps.getPropertyNameById(serverPrpId);
			
			return hdb.getHistory(ctxName, srcName,prpName, limit, from, to, withTags, null);
		}

		@Override
		public boolean historySupported() {
			return !historyInit;
		}

		@Override
		public long latestRecord(Integer serverPrpId) {
			if(historyInit)
				return -1;
			
			Integer serverSrcId = contextMaps.getSourceIdForPropertyId(serverPrpId);
			Integer serverCtxId = contextMaps.getContextIdForSourceId(serverSrcId);
			
			String ctxName = contextMaps.getContextById(serverCtxId).getName();
			String srcName = contextMaps.getSourceNameById(serverSrcId);
			String prpName = contextMaps.getPropertyNameById(serverPrpId);

			return hdb.getPrpTimestamp(ctxName, srcName, prpName, true);
		}
	}
	
	public static void main(String...args) {
		String serverPropertyFile = "context-server.properties";
		boolean needsProperties = false;
		
		if(args.length>0) {
			serverPropertyFile = args[0];
			needsProperties = true;
		}
		
		Properties props = new Properties();
		InputStream propsIS = ClassLoader.getSystemClassLoader().getResourceAsStream(serverPropertyFile);
		
		if(propsIS!=null) {
			System.out.println("Loading properties...");
			try {
				props.load(propsIS);
				propsIS.close();
			} catch (IOException e) {
				if(needsProperties) {
					System.err.println("There was an error loading the server-properties: " + e.getMessage());
					return;
				}
			}
		} else {
			if(needsProperties) {
				System.err.println("The server-properties could not be found!");
				return;
			}
		}
		
		// put in try-catch for restricted environments
		try {
			props.putAll(System.getProperties());
		} catch(Exception e) {
		}
		
		ServerThread st;
		try {
			st = new ServerThread(props);
			st.start();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
