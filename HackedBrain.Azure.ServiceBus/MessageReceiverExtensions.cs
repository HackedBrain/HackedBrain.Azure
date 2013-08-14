using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class MessageReceiverExtensions
	{
		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver)
		{
			if(messageReceiver == null)
			{
				throw new ArgumentNullException("messageReceiver");
			}
			
			return new MessageReceiverObservable(messageReceiver);
		}
	}
}
