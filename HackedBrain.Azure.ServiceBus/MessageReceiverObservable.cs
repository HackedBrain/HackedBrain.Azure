using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging
{
	public sealed class MessageReceiverObservable : IObservable<BrokeredMessage>, IDisposable
	{
		private MessageReceiver messageReceiver;
		private CancellationTokenSource messageReceptionCancellationTokenSource;
		private List<IObserver<BrokeredMessage>> observers;
		
		public MessageReceiverObservable(MessageReceiver messageReceiver)
		{
			this.messageReceiver = messageReceiver;
			this.observers = new List<IObserver<BrokeredMessage>>();
		}
		
		public IDisposable Subscribe(IObserver<BrokeredMessage> observer)
		{
			lock(this.observers)
			{
				bool isFirstObserver = this.observers.Count == 0;

				this.observers.Add(observer);

				if(isFirstObserver)
				{
					this.messageReceptionCancellationTokenSource = new CancellationTokenSource();

					this.ReceiveNextMessage();
				}
			}

			return new Subscription(this, observer);
		}

		public void Dispose()
		{
			if(Interlocked.Exchange(ref this.messageReceiver, null) == null)
			{
				if(this.messageReceiver == null)
				{
					throw new ObjectDisposedException("observer", "This subscription has already been disposed.");
				}
			}

			if(this.messageReceptionCancellationTokenSource != null)
			{
				this.messageReceptionCancellationTokenSource.Cancel();
			}

			foreach(IObserver<BrokeredMessage> observer in this.observers)
			{
				observer.OnCompleted();
			}
		}

		private void RemoveSubscription(IObserver<BrokeredMessage> observer)
		{
			lock(this.observers)
			{
				this.observers.Remove(observer);

				if(this.observers.Count == 0)
				{
					this.messageReceptionCancellationTokenSource.Cancel();
					this.messageReceptionCancellationTokenSource = null;
				}
			}
		}

		private void ReceiveNextMessage()
		{
			if(this.messageReceptionCancellationTokenSource != null
						&&
				!this.messageReceptionCancellationTokenSource.IsCancellationRequested)
			{
				Task.Factory.FromAsync<BrokeredMessage>(
					this.messageReceiver.BeginReceive,
					this.messageReceiver.EndReceive,
					null, 
					TaskCreationOptions.HideScheduler)
					.ContinueWith(receiveAntecedent =>
					{
						if(receiveAntecedent.IsFaulted)
						{
							this.NotifySubscribersOfError(receiveAntecedent.Exception);
						}
						else
						{
							this.NotifySubscribersAboutNextMessage(receiveAntecedent.Result);
						}

						this.ReceiveNextMessage();
					},
					this.messageReceptionCancellationTokenSource.Token);
			}
		}

		private void NotifySubscribersAboutNextMessage(BrokeredMessage nextMessage)
		{
			foreach(IObserver<BrokeredMessage> observer in this.GetSubscribersSnapshot())
			{
				if(!this.messageReceptionCancellationTokenSource.IsCancellationRequested)
				{
					observer.OnNext(nextMessage);
				}
			}
		}

		private void NotifySubscribersOfError(Exception exception)
		{
			foreach(IObserver<BrokeredMessage> observer in this.GetSubscribersSnapshot())
			{
				if(!this.messageReceptionCancellationTokenSource.IsCancellationRequested)
				{
					observer.OnError(exception);
				}
			}
		}

		private IObserver<BrokeredMessage>[] GetSubscribersSnapshot()
		{
			lock(this.observers)
			{
				return this.observers.ToArray();
			}
		}

		private sealed class Subscription : IDisposable
		{
			private MessageReceiverObservable parentMessageReceiverObservable;
			private IObserver<BrokeredMessage> observer;
			
			public Subscription(MessageReceiverObservable parentMessageReceiverObservable, IObserver<BrokeredMessage> observer)
			{
				this.parentMessageReceiverObservable = parentMessageReceiverObservable;
				this.observer = observer;
			}

			public void Dispose()
			{
				if(this.observer == null)
				{
					throw new ObjectDisposedException("observer", "This subscription has already been disposed.");
				}

				this.parentMessageReceiverObservable.RemoveSubscription(this.observer);

				this.parentMessageReceiverObservable = null;
				this.observer = null;
			}
		}
	}
}
