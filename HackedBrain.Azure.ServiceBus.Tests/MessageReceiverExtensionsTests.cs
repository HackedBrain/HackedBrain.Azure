using System;
using System.Reactive.Linq;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging.Tests
{
	public class MessageReceiverObservableExtensionsTests : IDisposable
	{
		private NamespaceManager namespaceManager;
		private QueueDescription queueDescription;
		private MessageSender messageSender;
		private MessageReceiver messageReceiver;

		public MessageReceiverObservableExtensionsTests()
		{
			TokenProvider tp = TokenProvider.CreateSharedSecretTokenProvider("owner", "AEKOXTG5x21S1ZIdOl3m5xU+EfR+WOUGrxKsaqDfgD0=");

			this.namespaceManager = new NamespaceManager(
				"sb://dmarsh-msdn.servicebus.windows.net",
				new NamespaceManagerSettings
				{
					TokenProvider = tp
				});

			string queueName = "MessageReceiverExtensionTests-" + Guid.NewGuid();

			this.queueDescription = this.namespaceManager.CreateQueue(queueName);

			MessagingFactory messagingFactory = MessagingFactory.Create(
				this.namespaceManager.Address,
				new MessagingFactorySettings
				{
					TokenProvider = tp
				});

			this.messageSender = messagingFactory.CreateMessageSender(this.queueDescription.Path);
			this.messageReceiver = messagingFactory.CreateMessageReceiver(this.queueDescription.Path);
		}

		public void Dispose()
		{
			this.messageSender.Close();
			this.messageReceiver.Close();

			this.namespaceManager.DeleteQueue(this.queueDescription.Path);
		}

		public class ToObservableFacts : MessageReceiverObservableExtensionsTests
		{
			[Fact]
			public void Should_Throw_ArgumentNullException_For_Null_MessageReceiver_Instance()
			{
				MessageReceiver messageReceiver = null;

				Assert.Throws<ArgumentNullException>(() =>
					{
						messageReceiver.WhenMessageReceived();
					});
			}

			[Fact]
			public void Should_Return_NonNull_IObservable_Instance()
			{
				IObservable<BrokeredMessage> messageReceiverObservable = this.messageReceiver.WhenMessageReceived();

				Assert.NotNull(messageReceiverObservable);
			}
		}

		public class ObservableMessageReceptionFacts : MessageReceiverObservableExtensionsTests
		{
			[Fact]
			public void Should_Receive_Expected_Message()
			{
				string newMessageId = Guid.NewGuid().ToString();

				this.messageSender.Send(new BrokeredMessage
					{
						MessageId = newMessageId
					});

				string receivedMessageId = this.messageReceiver.WhenMessageReceived().Take(1).Select(bm => bm.MessageId).First();

				Assert.Equal(newMessageId, receivedMessageId);
			}

			[Fact]
			public void Should_Receive_Expected_Messages_In_Order()
			{
				string firstMessageId = Guid.NewGuid().ToString();

				this.messageSender.Send(new BrokeredMessage
				{
					MessageId = firstMessageId
				});

				string secondMessageId = Guid.NewGuid().ToString();

				this.messageSender.Send(new BrokeredMessage
				{
					MessageId = secondMessageId
				});

				Assert.True(this.messageReceiver.WhenMessageReceived().Take(2).Select(bm => bm.MessageId).SequenceEqual(new[] { firstMessageId, secondMessageId }).First());
			}
		}
	}
}
