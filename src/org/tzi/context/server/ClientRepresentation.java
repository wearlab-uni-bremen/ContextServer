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

import java.io.ByteArrayOutputStream;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.TreeMap;

import org.tzi.context.common.Protocol;

public class ClientRepresentation {	
	private String name;
	private int id;
	private long lastMessageTime = -1L;
	private long lastPingTime = -1L;
	private long lastSendTime = -1L;
	public byte [] buffer = new byte [Protocol.maxDataSize];
	public int bindex = 0;
	
	public static class TransferRepresentation {
		private boolean transferFromClient = false;
		private int txFromClientSize;
		private int txFromClientPacketIndex;
		private ByteArrayOutputStream txFromClientBuffer;

		private boolean transferToClient = false;
		private int txToClientNumPackets;
		private byte [] txForClientData;
		
		
		public boolean isTransferFromClient() {
			return transferFromClient;
		}

		public boolean isTransferToClient() {
			return transferToClient;
		}
		
		public int getTransferFromClientSize() {
			return txFromClientSize;
		}
		
		public int getTransferFromClientPacketIndex() {
			return txFromClientPacketIndex;
		}
		
		
		public int getTransferFromClientCurrentSize() {
			if(!transferFromClient)
				return 0;
			
			return txFromClientBuffer.size();
		}
		
		
		public void appendTransferFromClient(byte [] buffer, int off, int len) {
			if(!transferFromClient)
				throw new RuntimeException("Not in transfer!");
			
			txFromClientBuffer.write(buffer, off, len);
			
			txFromClientPacketIndex++;
		}
		
		public int getTransferToClientSize() {
			return txForClientData.length;
		}
		
		public int getTransferToClientNumPackets() {
			if(!transferToClient)
				return 0;
			
			return txToClientNumPackets;
		}

		public int getPacketData(int n, byte [] buffer, int offs) {
			return Protocol.getPacketData(txForClientData, n, buffer, offs);
		}
		
	}
	
	private Map<Integer, TransferRepresentation> transferMap = new TreeMap<Integer, TransferRepresentation>();
	
	private Queue<String> incoming = new LinkedList<String>();
	private Queue<String> outgoing = new LinkedList<String>();
	
	public ClientRepresentation(String name, int id) {
		this.name = name;
		this.id = id;
	}

	public ClientRepresentation(String name) {
		this.name = name;
		this.id = -1;
	}
	
	public String getName() {
		return name;
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public long getLastMessageTime() {
		return lastMessageTime;
	}
	
	public void setLastMessageTime(long t) {
		lastMessageTime = t;
	}
	
	public long getLastSendTime() {
		return lastSendTime;
	}
	
	public void setLastSendTime(long t) {
		lastSendTime = t;
	}
	
	public long getLastPingTime() {
		return lastPingTime;
	}
	
	public void setLastPingTime(long t) {
		lastPingTime = t;
	}
	
	public void addMessage(String msg) {
		outgoing.add(msg);
	}
	
	public Queue<String> getMessageQueue() {
		return outgoing;
	}
	
	public Queue<String> getIncomingMessages() {
		return incoming;
	}
	
	public TransferRepresentation getTransferRepresentation(int tid) {
		return transferMap.get(tid);
	}
	
	public byte [] finishTransferFromClient(int tid) {
		TransferRepresentation tr = transferMap.get(tid);
		if(tr==null)
			throw new RuntimeException("No transfer with ID " + tid);
		
		if(!tr.transferFromClient)
			throw new RuntimeException("Not in transfer!");

		tr.transferFromClient = false;
		byte [] r = tr.txFromClientBuffer.toByteArray();
		
		transferMap.remove(tid);

		return r;
	}
	
	public void cancelTransferFromClient(int tid) {
		TransferRepresentation tr = transferMap.get(tid);
		
		if(tr==null)
			return;

		tr.transferFromClient = false;
		
		transferMap.remove(tr);
	}
	
	public void startTransferToClient(int tid, byte [] data) {
		TransferRepresentation tr = transferMap.get(tid);
		if(tr!=null)
			throw new RuntimeException("Transfer with ID " + tid + " already exists!");
		
		tr = new TransferRepresentation();
		transferMap.put(tid, tr);

		tr.txForClientData = data;
		tr.txToClientNumPackets = (data.length + (Protocol.txMaxData-1)) / Protocol.txMaxData;
		tr.transferToClient = true;
	}
	
	
	public TransferRepresentation startTransferFromClient(int tid, int size) {
		TransferRepresentation tr = transferMap.get(tid);
		if(tr!=null)
			throw new RuntimeException("Transfer with ID " + tid + " already exists!");
		
		tr = new TransferRepresentation();
		transferMap.put(tid, tr);
		
		tr.txFromClientSize = size;
		tr.txFromClientPacketIndex = 0;
		tr.txFromClientBuffer = new ByteArrayOutputStream(size);
		tr.transferFromClient = true;
		
		return tr;
	}
	
	public void cancelTransferToClient(int tid) {
		TransferRepresentation tr = transferMap.get(tid);
		
		if(tr==null)
			return;

		tr.transferToClient = false;
		tr.txForClientData = null;

		transferMap.remove(tid);
	}

	public void finishTransferToClient(int tid) {
		TransferRepresentation tr = transferMap.get(tid);
		
		if(tr==null)
			return;

		tr.transferToClient = false;
		tr.txForClientData = null;
		
		transferMap.remove(tid);
	}

}
