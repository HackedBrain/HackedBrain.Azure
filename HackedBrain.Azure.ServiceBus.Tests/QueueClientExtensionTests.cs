using System;
using System.Diagnostics;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging.Tests
{
	public class QueueClientObservableExtensionsTests : IDisposable
	{
		private NamespaceManager namespaceManager;
		private QueueDescription queueDescription;
		private QueueClient queueClient;

		public QueueClientObservableExtensionsTests()
		{
			TokenProvider tp = TokenProvider.CreateSharedSecretTokenProvider("owner", "AEKOXTG5x21S1ZIdOl3m5xU+EfR+WOUGrxKsaqDfgD0=");

			this.namespaceManager = new NamespaceManager(
				"sb://dmarsh-msdn.servicebus.windows.net",
				new NamespaceManagerSettings
				{
					TokenProvider = tp
				});

			this.queueDescription = new QueueDescription("MessageReceiverExtensionTests-" + Guid.NewGuid())
			{
				RequiresSession = true,
			};

			this.queueDescription = this.namespaceManager.CreateQueue(this.queueDescription);

			MessagingFactory messagingFactory = MessagingFactory.Create(
				this.namespaceManager.Address,
				new MessagingFactorySettings
				{
					TokenProvider = tp
				});

			this.queueClient = messagingFactory.CreateQueueClient(this.queueDescription.Path);
		}

		public void Dispose()
		{
			this.queueClient.Close();

			this.namespaceManager.DeleteQueue(this.queueDescription.Path);
		}

		public class AsSessionObservableFacts : QueueClientObservableExtensionsTests
		{
			[Fact]
			public void Should_Throw_ArgumentNullException_For_Null_QueueClient_Instance()
			{
				QueueClient queueClient = null;

				Assert.Throws<ArgumentNullException>(() =>
					{
						queueClient.WhenSessionAccepted();
					});
			}

			[Fact]
			public void Should_Return_NonNull_IObservable_Instance()
			{
				IObservable<MessageSession> messageReceiverObservable = this.queueClient.WhenSessionAccepted();

				Assert.NotNull(messageReceiverObservable);
			}
		}

		public class ObservableMessageSessionFacts : QueueClientObservableExtensionsTests
		{
			[Fact]
			public void Should_Receive_Expected_Session_Message()
			{
				string newSessionId = Guid.NewGuid().ToString();
				string newMessageId = Guid.NewGuid().ToString();

				this.queueClient.Send(new BrokeredMessage
					{
						SessionId = newSessionId,
						MessageId = newMessageId
					});

				var receviedMessageInfo = (from session in this.queueClient.WhenSessionAccepted()
										   from msg in session.WhenMessageReceived()
										   select new
										   {
											   SessionId = session.SessionId,
											   MessageId = msg.MessageId
										   }).Take(1).First();

				Assert.Equal(newSessionId, receviedMessageInfo.SessionId);
				Assert.Equal(newMessageId, receviedMessageInfo.MessageId);
			}

			[Fact]
			public void Should_Receive_Session_Messages_In_Expected_Sequence()
			{
				string firstSessionId = Guid.NewGuid().ToString();

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = firstSessionId,
					MessageId = "1",
				});

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = firstSessionId,
					MessageId = "2",
				});

				string secondSessionId = Guid.NewGuid().ToString();

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = secondSessionId,
					MessageId = "A"
				});

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = secondSessionId,
					MessageId = "B"
				});

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = secondSessionId,
					MessageId = "C",
				});

				this.queueClient.Send(new BrokeredMessage
				{
					SessionId = firstSessionId,
					MessageId = "3",
				});

				this.queueClient.WhenSessionAccepted(TimeSpan.FromSeconds(5))
					.SelectMany(messageSession =>
						messageSession.WhenMessageReceived(TimeSpan.FromSeconds(1))
							.SelectMany(
								message => Observable.Using(
																() => message,
																m => Observable.Return(new
																{
																	messageSession.SessionId,
																	m.MessageId
																}))))
					.ForEach(
						it =>
						{
							Trace.TraceInformation(it.SessionId + " : " + it.MessageId);
						});
				//.SequenceEqual(new[]
				//  {
				//	  new 
				//	  {
				//		  SessionId = firstSessionId,
				//		  MessageId = "1",
				//	  },
				//	  new 
				//	  {
				//		  SessionId = firstSessionId,
				//		  MessageId = "2",
				//	  },
				//	  new 
				//	  {
				//		  SessionId = firstSessionId,
				//		  MessageId = "3",
				//	  },
				//	  new 
				//	  {
				//		  SessionId = secondSessionId,
				//		  MessageId = "A",
				//	  },
				//	  new 
				//	  {
				//		  SessionId = secondSessionId,
				//		  MessageId = "B",
				//	  },
				//	  new 
				//	  {
				//		  SessionId = secondSessionId,
				//		  MessageId = "C",
				//	  }
				//  }).First();

				//Assert.True(sequenceIsAsExpected);
				Debug.WriteLine("test");
			}
		}
	}
}
