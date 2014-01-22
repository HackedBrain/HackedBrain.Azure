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
	internal static class MessagingClientExtensionHelpers
	{
		public static IObservable<MessageSession> WhenSessionAccepted(this MessageClientEntity messageClientEntity, Func<Task<MessageSession>> acceptMessageSessionImpl, IScheduler scheduler)
		{
#if DEBUG
			
			if(acceptMessageSessionImpl == null)
			{
				throw new ArgumentNullException("acceptMessageSessionImpl");
			}

			if(messageClientEntity == null)
			{
				throw new ArgumentNullException("messageClientEntity");
			}

			if(scheduler == null)
			{
				throw new ArgumentNullException("scheduler");
			}

#endif

			IObservable<MessageSession> brokeredMessages = Observable.Create<MessageSession>(
				observer =>
				{
					Func<IScheduler, CancellationToken, Task<IDisposable>> messageSessionPump = null;

					messageSessionPump = async (previousScheduler, cancellationToken) =>
					{
						IDisposable disposable;
						
						if(!cancellationToken.IsCancellationRequested)
						{
							if(messageClientEntity.IsClosed)
							{
								observer.OnCompleted();

								disposable = Disposable.Empty;
							}
							else
							{
								try
								{
									MessageSession nextMessageSession = await acceptMessageSessionImpl();

									if(nextMessageSession != null)
									{
										observer.OnNext(nextMessageSession);

										disposable = ImmediateScheduler.Instance.ScheduleAsync(messageSessionPump);
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

					return scheduler.ScheduleAsync(messageSessionPump);
				});

			return Observable.Using(() => Disposable.Create(messageClientEntity.Close), _ => brokeredMessages);
		}
	}
}
