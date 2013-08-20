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

			Func<Task<BrokeredMessage>> receiveMessage = receiveWaitTime != TimeSpan.MinValue ?
						new Func<Task<BrokeredMessage>>(() => messageReceiver.ReceiveAsync(receiveWaitTime)) :
						new Func<Task<BrokeredMessage>>(() => messageReceiver.ReceiveAsync());

			return MessageReceiverExtensions.ObservableFromAsyncReceiverDetails(() => messageReceiver.IsClosed, () => messageReceiver.Close(), receiveMessage, scheduler);
		}

		public static IObservable<BrokeredMessage> AsObservable(this QueueClient queueClient)
		{
			return queueClient.AsObservable(TimeSpan.MinValue, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> AsObservable(this QueueClient queueClient, IScheduler scheduler)
		{
			return queueClient.AsObservable(TimeSpan.MinValue, scheduler);
		}

		public static IObservable<BrokeredMessage> AsObservable(this QueueClient queueClient, TimeSpan receiveWaitTime)
		{
			return queueClient.AsObservable(receiveWaitTime, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> AsObservable(this QueueClient queueClient, TimeSpan receiveWaitTime, IScheduler scheduler)
		{
			if(queueClient == null)
			{
				throw new ArgumentNullException("queueClient");
			}

			Func<Task<BrokeredMessage>> receiveMessage = receiveWaitTime != TimeSpan.MinValue ?
						new Func<Task<BrokeredMessage>>(() => queueClient.ReceiveAsync(receiveWaitTime)) :
						new Func<Task<BrokeredMessage>>(() => queueClient.ReceiveAsync());

			return MessageReceiverExtensions.ObservableFromAsyncReceiverDetails(() => queueClient.IsClosed, () => queueClient.Close(), receiveMessage, scheduler);
		}

		private static IObservable<BrokeredMessage> ObservableFromAsyncReceiverDetails(Func<bool> receiverIsClosed, Action receiverClose, Func<Task<BrokeredMessage>> receiveMessage, IScheduler scheduler)
		{
			IObservable<BrokeredMessage> brokeredMessages = Observable.Create<BrokeredMessage>(
				observer =>
				{
					Func<IScheduler, CancellationToken, Task<IDisposable>> messagePump = null;

					messagePump = async (previousScheduler, cancellationToken) =>
					{
						cancellationToken.ThrowIfCancellationRequested();

						if(receiverIsClosed())
						{
							observer.OnCompleted();

							return Disposable.Empty;
						}
						else
						{
							try
							{
								BrokeredMessage nextMessage = await receiveMessage();

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

			return Observable.Using(() => Disposable.Create(receiverClose), _ => brokeredMessages);
		}
	}
}
