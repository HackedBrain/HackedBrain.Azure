using System;
using System.Reactive.Concurrency;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class SubscriptionClientExtensions
	{
		public static IObservable<MessageSession> WhenSessionAccepted(this SubscriptionClient subscriptionClient)
		{
			return subscriptionClient.WhenSessionAccepted(ImmediateScheduler.Instance);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this SubscriptionClient subscriptionClient, TimeSpan sessionWaitTimeout)
		{
			return subscriptionClient.WhenSessionAccepted(sessionWaitTimeout, ImmediateScheduler.Instance);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this SubscriptionClient subscriptionClient, IScheduler scheduler)
		{
			return subscriptionClient.WhenSessionAccepted(TimeSpan.MinValue, scheduler);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this SubscriptionClient subscriptionClient, TimeSpan sessionWaitTimeout, IScheduler scheduler)
		{
			if(subscriptionClient == null)
			{
				throw new ArgumentNullException("subscriptionClient");
			}

			Func<Task<MessageSession>> acceptMessageSessionFunc = sessionWaitTimeout != TimeSpan.MinValue ?
				new Func<Task<MessageSession>>(() => subscriptionClient.AcceptMessageSessionAsync(sessionWaitTimeout)) :
				subscriptionClient.AcceptMessageSessionAsync;

			return subscriptionClient.WhenSessionAccepted(acceptMessageSessionFunc, scheduler);
		}
	}
}
