using System;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Moq;
using Xunit;

namespace HackedBrain.WindowsAzure.ServiceBus.Messaging.Tests
{
	public class MessageReceiverObservableExtensionsTests
	{
		public class AsObservableFacts : IDisposable
		{
			private MessageReceiver messageReceiver;

			public AsObservableFacts()
			{
				TokenProvider tp = TokenProvider.CreateSharedSecretTokenProvider("owner", new byte[] { 1, 2, 3 });

				MessagingFactorySettings mfs = new MessagingFactorySettings
				{
					TokenProvider = tp
				};

				MessagingFactory mf = MessagingFactory.Create(new Uri("sb://unittest"), mfs);
				this.messageReceiver = mf.CreateMessageReceiver("test-queue-path");
			}

			public void Dispose()
			{
				
			}
			
			[Fact]
			public void Should_Throw_ArgumentNullException_For_Null_MessageReceiver_Instance()
			{
				MessageReceiver messageReceiver = null;

				Assert.Throws<ArgumentNullException>(() =>
					{
						messageReceiver.AsObservable();
					});
			}

			[Fact]
			public void Should_Return_NonNull_MessageReceiverObservable_Instance()
			{
				IObservable<BrokeredMessage> messageReceiverObservable = this.messageReceiver.AsObservable();

				Assert.NotNull(messageReceiverObservable);
				Assert.IsType<MessageReceiverObservable>(messageReceiverObservable);
			}
		}
	}
}
