using System;
using System.Reactive.Linq;
using System.Threading.Tasks;
using HackedBrain.Azure.ServiceBus.Tests;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Xunit;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging.Tests
{
	public class MessageReceiverObservableExtensionsTests : IDisposable
	{
		
		private QueueDescription queueDescription;
		private MessageSender messageSender;
		private MessageReceiver messageReceiver;
        private AzureServiceBusTestUtilities azureServiceBusTestUtilities;

		public MessageReceiverObservableExtensionsTests()
		{
            this.azureServiceBusTestUtilities = AzureServiceBusTestUtilities.Create();

			string queueName = "MessageReceiverExtensionTests-" + Guid.NewGuid();

			this.queueDescription = this.azureServiceBusTestUtilities.NamespaceManager.CreateQueue(queueName);

            MessagingFactory messagingFactory = this.azureServiceBusTestUtilities.MessagingFactory;

			this.messageSender = messagingFactory.CreateMessageSender(this.queueDescription.Path);
			this.messageReceiver = messagingFactory.CreateMessageReceiver(this.queueDescription.Path);
		}

		public void Dispose()
		{
			this.messageSender.Close();
			this.messageReceiver.Close();

			this.azureServiceBusTestUtilities.NamespaceManager.DeleteQueue(this.queueDescription.Path);
		}

		public class WhenMessageReceivedFacts : MessageReceiverObservableExtensionsTests
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
			public async Task Should_Receive_Expected_Message()
			{
				string newMessageId = Guid.NewGuid().ToString();

				this.messageSender.Send(new BrokeredMessage
					{
						MessageId = newMessageId
					});

				string receivedMessageId = await this.messageReceiver.WhenMessageReceived().Take(1).Select(bm => bm.MessageId).FirstAsync();

				Assert.Equal(newMessageId, receivedMessageId);
			}

			[Fact]
			public async Task Should_Receive_Expected_Messages_In_Order()
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

				bool sequencesAreEqual = await this.messageReceiver.WhenMessageReceived().Take(2).Select(bm => bm.MessageId).SequenceEqual(new[] { firstMessageId, secondMessageId }).FirstAsync();

				Assert.True(sequencesAreEqual);
			}
		}
	}
}
