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
			return queueClient.WhenSessionAccepted(TaskPoolScheduler.Default);
		}

		public static IObservable<MessageSession> WhenSessionAccepted(this QueueClient queueClient, TimeSpan sessionWaitTimeout)
		{
			return queueClient.WhenSessionAccepted(sessionWaitTimeout, TaskPoolScheduler.Default);
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

			return QueueClientExtensions.WhenSessionAccepted(acceptMessageSessionFunc, () => queueClient.IsClosed, queueClient.Close, scheduler);
		}

		private static IObservable<MessageSession> WhenSessionAccepted(Func<Task<MessageSession>> acceptMessageSessionImpl, Func<bool> isClosedImpl, Action closeImpl, IScheduler scheduler)
		{
#if DEBUG
			
			if(acceptMessageSessionImpl == null)
			{
				throw new ArgumentNullException("acceptMessageSessionImpl");
			}

			if(isClosedImpl == null)
			{
				throw new ArgumentNullException("isClosedImpl");
			}

			if(closeImpl == null)
			{
				throw new ArgumentNullException("closeImpl");
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
						cancellationToken.ThrowIfCancellationRequested();

						IDisposable disposable;

						if(isClosedImpl())
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

						return disposable;
					};

					return scheduler.ScheduleAsync(messageSessionPump);
				});

			return Observable.Using(() => Disposable.Create(() => closeImpl()), _ => brokeredMessages);
		}
	}
}
