using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class QueueClientExtensions
	{
		public static IObservable<MessageSession> WhenSessionAccepted(this QueueClient queueClient)
		{
			return queueClient.WhenSessionAccepted(ImmediateScheduler.Instance);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this QueueClient queueClient, TimeSpan sessionWaitTimeout)
		{
			return queueClient.WhenSessionAccepted(sessionWaitTimeout, ImmediateScheduler.Instance);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this QueueClient queueClient, IScheduler scheduler)
		{
			return queueClient.WhenSessionAccepted(TimeSpan.MinValue, scheduler);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this QueueClient queueClient, TimeSpan sessionWaitTimeout, IScheduler scheduler)
		{
			if(queueClient == null)
			{
				throw new ArgumentNullException("queueClient");
			}
			
			Func<Task<MessageSession>> acceptMessageSessionFunc = sessionWaitTimeout != TimeSpan.MinValue ?
				new Func<Task<MessageSession>>(() => queueClient.AcceptMessageSessionAsync(sessionWaitTimeout)) :
				queueClient.AcceptMessageSessionAsync;

			return queueClient.WhenSessionAccepted(acceptMessageSessionFunc, scheduler);
		}
	}
}
