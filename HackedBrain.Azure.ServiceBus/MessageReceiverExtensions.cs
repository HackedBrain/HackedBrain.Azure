using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class MessageReceiverExtensions
	{
		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver)
		{
			return messageReceiver.AsObservable(CancellationToken.None, Scheduler.Immediate);
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, CancellationToken cancellationToken)
		{
			return messageReceiver.AsObservable(cancellationToken, Scheduler.Immediate);
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, IScheduler scheduler)
		{
			return messageReceiver.AsObservable(CancellationToken.None, scheduler);
		}

		public static IObservable<BrokeredMessage> AsObservableFullAsync(this MessageReceiver messageReceiver, CancellationToken cancellationToken, IScheduler scheduler)
		{
			if(messageReceiver == null)
			{
				throw new ArgumentNullException("messageReceiver");
			}

			Func<IScheduler, CancellationToken, Task<IDisposable>> recursiveMessageReceiver = null;

			return Observable.Create<BrokeredMessage>(
				observer =>
				{
					recursiveMessageReceiver = async (recursiveScheduler, recursiveCancellationToken) =>
					{
						try
						{
							BrokeredMessage message = await messageReceiver.ReceiveAsync();

							observer.OnNext(message);
						}
						catch(Exception exception)
						{
							observer.OnError(exception);
						}

						return recursiveScheduler.ScheduleAsync(recursiveMessageReceiver);
					};

					return scheduler.ScheduleAsync(recursiveMessageReceiver);
				});
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, CancellationToken cancellationToken, IScheduler scheduler)
		{
			if(messageReceiver == null)
			{
				throw new ArgumentNullException("messageReceiver");
			}

			return Observable.Create<BrokeredMessage>(
				observer =>
				{
					return scheduler.Schedule(
						recurse =>
						{
							try
							{
								BrokeredMessage message = messageReceiver.Receive();

								observer.OnNext(message);
							}
							catch(Exception exception)
							{
								observer.OnError(exception);
							}

							scheduler.Schedule(recurse);
						});
				});
		}
	}
}
