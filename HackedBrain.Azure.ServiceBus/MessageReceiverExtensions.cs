using System;
using System.Collections.Generic;
using System.Linq;
using System.Reactive.Concurrency;
using System.Reactive.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using System.Reactive.PlatformServices;
using System.Reactive.Disposables;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class MessageReceiverExtensions
	{
		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver)
		{
			return messageReceiver.AsObservable(TimeSpan.MinValue, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, TimeSpan receiveWaitTime)
		{
			return messageReceiver.AsObservable(receiveWaitTime, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, IScheduler scheduler)
		{
			return messageReceiver.AsObservable(TimeSpan.MinValue, scheduler);
		}

		public static IObservable<BrokeredMessage> AsObservable(this MessageReceiver messageReceiver, TimeSpan receiveWaitTime, IScheduler scheduler)
		{
			if(messageReceiver == null)
			{
				throw new ArgumentNullException("messageReceiver");
			}

			IObservable<BrokeredMessage> brokeredMessages = Observable.Create<BrokeredMessage>(
				observer =>
				{
					Func<Task<BrokeredMessage>> receiveCall = receiveWaitTime != TimeSpan.MinValue ? 
						new Func<Task<BrokeredMessage>>(() => messageReceiver.ReceiveAsync(receiveWaitTime)) :
						new Func<Task<BrokeredMessage>>(() => messageReceiver.ReceiveAsync());

					Func<IScheduler, CancellationToken, Task<IDisposable>> messagePump = null;

					messagePump = async (previousScheduler, cancellationToken) =>
					{
						cancellationToken.ThrowIfCancellationRequested();

						if(messageReceiver.IsClosed)
						{
							observer.OnCompleted();

							return Disposable.Empty;
						}
						else
						{
							try
							{
								BrokeredMessage nextMessage = await receiveCall();

								observer.OnNext(nextMessage);
							}
							catch(Exception exception)
							{
								observer.OnError(exception);
							}

							return ImmediateScheduler.Instance.ScheduleAsync(messagePump);
						}						
					};

					return scheduler.ScheduleAsync(messagePump);
				});

			return Observable.Using(() => Disposable.Create(() => messageReceiver.Close()), _ => brokeredMessages);
		}
	}
}
