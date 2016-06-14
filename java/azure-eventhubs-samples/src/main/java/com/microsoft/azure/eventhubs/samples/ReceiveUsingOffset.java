/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.concurrent.ExecutionException;
import java.util.logging.*;

import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;

public class ReceiveUsingOffset
{
	public static void main(String[] args)
			throws ServiceBusException, ExecutionException, InterruptedException, IOException
	{
		final String namespaceName = "<namespace>";
		final String eventHubName = "<eventhub>";
		final String sasKeyName = "<keyname>";
		final String sasKey = "<key>";
		ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);

		EventHubClient ehClient = EventHubClient.createFromConnectionString(connStr.toString()).get();


		// receiver
		String partitionId = "0"; // API to get PartitionIds will be released as part of V0.2
		PartitionReceiver receiver = ehClient.createEpochReceiver(
				EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
				partitionId,
				"12312",
				false,
				1).get();

		try {
			Iterable<EventData> receivedEvents = receiver.receiveSync(1);
			for (EventData receivedEvent : receivedEvents) {
				System.out.println(receivedEvent);
				System.out.println(new String(receivedEvent.getBody(), Charset.defaultCharset()));
			}
		} catch(Exception e) {
			System.out.println(e.getMessage());
		} finally {
			receiver.close().get();
		}
	}
}
