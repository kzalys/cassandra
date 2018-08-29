/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.repair;

import java.net.InetAddress;
import com.google.common.util.concurrent.AbstractFuture;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.AbstractFuture;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.net.IAsyncCallbackWithFailure;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.repair.messages.RepairMessage;
import org.apache.cassandra.repair.messages.ValidationComplete;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.utils.MerkleTree;
import org.apache.cassandra.service.ActiveRepairService;

/**
 * ValidationTask sends {@link ValidationRequest} to a replica.
 * When a replica sends back message, task completes.
 */
public class ValidationTask extends AbstractFuture<TreeResponse> implements Runnable
{
    private final RepairJobDesc desc;
    private final InetAddress endpoint;
    private final int gcBefore;

    public ValidationTask(RepairJobDesc desc, InetAddress endpoint, int gcBefore)
    {
        this.desc = desc;
        this.endpoint = endpoint;
        this.gcBefore = gcBefore;
    }

    class ValidationCallBack implements IAsyncCallbackWithFailure<RepairMessage>
    {
        @Override
        public void onFailure(InetAddress from)
        {
            setException(new RepairException(desc, "Validation failed in " + endpoint));
        }

        @Override
        public void response(MessageIn<RepairMessage> msg)
        {
            //Receive MerkleTrees from replica node.
            RepairMessage repairMessage = msg.payload;
            RepairJobDesc desc = repairMessage.desc;
            UUID sessionId = desc.sessionId;
            RepairSession repairSession = ActiveRepairService.instance.getSession(sessionId);
            if (repairSession == null)
            {
                return;
            }

            //MerkleTrees that is sent from replica. Null if validation failed on replica node.
            ValidationComplete validaton = (ValidationComplete) repairMessage;
            MerkleTree trees = validaton.tree;
            repairSession.validationComplete(desc, msg.from, trees);
            if (trees == null)
            {
                setException(new RepairException(desc, "Validation failed in " + endpoint));
            }
            else
            {
                set(new TreeResponse(endpoint, trees));
            }
        }

        @Override
        public boolean isLatencyForSnitch()
        {
            return false;
        }
    }

    /**
     * Send ValidationRequest to replica
     */
    public void run()
    {
        ValidationRequest request = new ValidationRequest(desc, gcBefore);
        ValidationCallBack cb = new ValidationCallBack();
        MessagingService.instance().sendRR(request.createMessage(), endpoint, cb, TimeUnit.SECONDS.toMillis
                        (DatabaseDescriptor.getRepairValidationRequestTimeout()), true);
    }
}
