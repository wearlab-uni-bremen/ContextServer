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

public interface ContextSubscriptionManager {
	public Integer addClientProxyListener(Integer cId, ClientProxyListener cpl);
	public void removeClientSubscription(Integer cId, Integer sId);
	public void removeAllClientSubscriptions(Integer cId);
	public void setShortStateClientSubscription(Integer cId, Integer sId, boolean shortFormat);
	public void setShortStateAllClientSubscription(Integer cId, boolean shortFormat);
	public boolean getShortStateClientSubscription(Integer cId, Integer sId);
}
