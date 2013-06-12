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

import java.nio.charset.Charset;

import org.tzi.context.common.Protocol;
import org.tzi.context.common.Util;
import org.tzi.context.server.ClientRepresentation;

public class CmdTransferHandler implements ServerCommand {
	
	
	private Charset asciiCharset = Charset.forName("ASCII");
	private byte [] txBuffer = new byte [Protocol.maxDataSize];

	@Override
	public ServerCommandResult processCommand(ServerCommandParameters scp) {
		ServerCommandResult scr = new ServerCommandResult();
		int tid;
		int packet;
 		byte [] packetData;
		int size = 0;
		int dataIndex;
		int txLen;
		ClientRepresentation.TransferRepresentation tr;

		switch(scp.command) {
		case TXPACKET:
			scr.usage = "TX <TID> (0 <SIZE> | <N > 0>) <DATA>";
			
			if(scp.words.length!=4 && scp.words.length!=5) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			try {
				tid = Integer.parseInt(scp.words[1]);
				
				packet = Integer.parseInt(scp.words[2]);
				
				if(packet < 0)
					throw new NumberFormatException();
				
			} catch(NumberFormatException nfe) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			dataIndex = 3;
			
			if(packet==0) {
				try {
					size = Integer.parseInt(scp.words[3]);
					
					if(size < 1)
						throw new NumberFormatException();
					
				} catch(NumberFormatException nfe) {
					scr.commandError = true;
					scr.errorMsg = scr.usage;
					break;
				}
				
				dataIndex = 4;
			}
			
			tr = scp.cr.getTransferRepresentation(tid);
			
			if(packet!=0 && !tr.isTransferFromClient()) {
				scr.commandError = true;
				scr.errorMsg = "Not in transfer!";
				break;
			}
			
			if(packet==0) {
				if(tr != null) {
					scr.commandError = true;
					scr.errorMsg = "Already in transfer for ID " + tid;
					break;
				}
				
				tr = scp.cr.startTransferFromClient(tid, size);
			}
			
			if(tr==null) {
				scr.commandError = true;
				scr.errorMsg = "No transfer for ID " + tid;
				break;
			}
			
			int neededPacket = tr.getTransferFromClientPacketIndex();
			
			if(packet < neededPacket) {
				scr.reply.append("(already received)");
				break;
			}

			if(packet > neededPacket) {
				scp.cr.addMessage(Protocol.TXRESEND + " " + tid + " " + neededPacket);
				scr.reply.append("(packet lost, resend-request-follows)");
				scr.writeOK = false;
				break;
			}

			scr.writeOK = true;

			packetData = scp.words[dataIndex].getBytes(asciiCharset);
			tr.appendTransferFromClient(packetData, 0, packetData.length);
			scp.cr.addMessage(Protocol.TXACK + " " + tid + " " + packet);
			
			if(tr.getTransferFromClientCurrentSize() >= tr.getTransferFromClientSize()) {
				byte [] fullMessage = scp.cr.finishTransferFromClient(tid);
				String message = Util.urldecode(new String(fullMessage, asciiCharset));
				scp.cr.getIncomingMessages().add(message);
			}
			
			break;
		case TXACK:
			scr.usage = "TXACK <TID> <N>";
			
			if(scp.words.length!=3) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			try {
				tid = Integer.parseInt(scp.words[1]);
				
				packet = Integer.parseInt(scp.words[2]);
				
				if(packet < 0)
					throw new NumberFormatException();
				
			} catch(NumberFormatException nfe) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			tr = scp.cr.getTransferRepresentation(tid);
			
			if(tr == null || !tr.isTransferToClient()) {
				scr.commandError = true;
				scr.errorMsg = "No current transfer!";
				break;
			}

			packet++;
			txLen = tr.getPacketData(packet, txBuffer, 0);

			if(txLen > 0) {
				scp.cr.addMessage(Protocol.TXPACKET + " " + tid + " " + packet + " " + new String(txBuffer, 0, txLen, asciiCharset));
			} else {
				scp.cr.finishTransferToClient(tid);
			}
			
			scr.writeOK = true;

			break;
		case TXCANCEL:
			scr.usage = "TXCANCEL <TID>";
			
			if(scp.words.length!=3) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			try {
				tid = Integer.parseInt(scp.words[1]);
			} catch(NumberFormatException nfe) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			tr = scp.cr.getTransferRepresentation(tid);
			
			if(tr==null) {
				scr.reply.append("(no transfer)");
			} else {
				if(tr.isTransferFromClient()) {
					scr.writeOK = true;
					scp.cr.cancelTransferFromClient(tid);
				}
				if(tr.isTransferToClient()) {
					scr.writeOK = true;
					scp.cr.cancelTransferToClient(tid);
				} else {
					scr.reply.append("(no transfer)");
				}
			}
			break;
		case TXRESEND:
			scr.usage = "TXRESEND <TID> <N>";
			
			if(scp.words.length!=3) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			try {
				tid = Integer.parseInt(scp.words[1]);

				packet = Integer.parseInt(scp.words[2]);
				
				if(packet < 0)
					throw new NumberFormatException();
				
			} catch(NumberFormatException nfe) {
				scr.commandError = true;
				scr.errorMsg = scr.usage;
				break;
			}
			
			tr = scp.cr.getTransferRepresentation(tid);
			
			if(tr == null || !tr.isTransferToClient()) {
				scr.commandError = true;
				scr.errorMsg = "No current transfer!";
				break;
			}
			
			if(packet >= tr.getTransferToClientNumPackets()) {
				scr.commandError = true;
				scr.errorMsg = "No such packet: " + packet;
				break;
			}
			
			txLen = tr.getPacketData(packet, txBuffer, 0);
			
			if(txLen > 0) {
				scp.cr.addMessage(Protocol.TXPACKET + " " + tid + " " + packet + " " + (packet == 0 ? Integer.toString(tr.getTransferToClientSize()) + " " : "") + new String(txBuffer, 0, txLen, asciiCharset));
			}
			
			scr.writeOK = true;
			
			break;
		default:
			throw new RuntimeException("Invalid command for TransferHandler!");
		}
		
		
		
		return scr;
	}

}
