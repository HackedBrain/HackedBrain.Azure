﻿using System;
using System.Reactive.Concurrency;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public static class MessageReceiverExtensions
	{
		public static IObservable<BrokeredMessage> WhenMessageReceived(this MessageReceiver messageReceiver)
		{
			return messageReceiver.WhenMessageReceived(TimeSpan.MinValue, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> WhenMessageReceived(this MessageReceiver messageReceiver, TimeSpan receiveWaitTime)
		{
			return messageReceiver.WhenMessageReceived(receiveWaitTime, ImmediateScheduler.Instance);
		}

		public static IObservable<BrokeredMessage> WhenMessageReceived(this MessageReceiver messageReceiver, IScheduler scheduler)
		{
			return messageReceiver.WhenMessageReceived(TimeSpan.MinValue, scheduler);
		}

		public static IObservable<BrokeredMessage> WhenMessageReceived(this MessageReceiver messageReceiver, TimeSpan receiveWaitTime, IScheduler scheduler)
		{
			if(messageReceiver == null)
			{
				throw new ArgumentNullException("messageReceiver");
			}

			IObservable<BrokeredMessage> brokeredMessages = Observable.Create<BrokeredMessage>(
				observer =>
				{
					Func<Task<BrokeredMessage>> receiveFunc = receiveWaitTime == TimeSpan.MinValue ? messageReceiver.ReceiveAsync : new Func<Task<BrokeredMessage>>(() => messageReceiver.ReceiveAsync(receiveWaitTime));

					Func<IScheduler, CancellationToken, Task<IDisposable>> messagePump = null;

					messagePump = async (previousScheduler, cancellationToken) =>
					{
						IDisposable disposable;

						if(!cancellationToken.IsCancellationRequested)
						{
							if(messageReceiver.IsClosed)
							{
								observer.OnCompleted();

								disposable = Disposable.Empty;
							}
							else
							{
								try
								{
									BrokeredMessage nextMessage = await receiveFunc();

									if(nextMessage != null)
									{
										observer.OnNext(nextMessage);

										disposable = ImmediateScheduler.Instance.ScheduleAsync(messagePump);
									}
									else
									{
										observer.OnCompleted();

										disposable = Disposable.Empty;
									}
								}
								catch(OperationCanceledException)
								{
									observer.OnCompleted();

									disposable = Disposable.Empty;
								}
								catch(TimeoutException)
								{
									observer.OnCompleted();

									disposable = Disposable.Empty;
								}
								catch(Exception exception)
								{
									observer.OnError(exception);

									disposable = Disposable.Empty;
								}
							}
						}
						else
						{
							observer.OnCompleted();
							
							disposable = Disposable.Empty;
						}

						return disposable;
					};

					return scheduler.ScheduleAsync(messagePump);
				});

			return Observable.Using(() => Disposable.Create(() => messageReceiver.Close()), _ => brokeredMessages);
		}
	}
}
