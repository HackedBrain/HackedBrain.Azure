using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using HackedBrain.WindowsAzure.ServiceBus.Messaging;

namespace HackedBrain.Azure.ServiceBus.Samples
{
	class Program
	{
		static void Main(string[] args)
		{
			NamespaceManager namespaceManager;
			QueueDescription queueDescription;
			QueueClient queueClient;

			TokenProvider tp = TokenProvider.CreateSharedSecretTokenProvider("owner", "AEKOXTG5x21S1ZIdOl3m5xU+EfR+WOUGrxKsaqDfgD0=");

			namespaceManager = new NamespaceManager(
				"sb://dmarsh-msdn.servicebus.windows.net",
				new NamespaceManagerSettings
				{
					TokenProvider = tp
				});

			string queuePath = "MessageReceiverConsoleApp";

			if(namespaceManager.QueueExists(queuePath))
			{
				namespaceManager.DeleteQueue(queuePath);
			}

			queueDescription = new QueueDescription(queuePath)
			{
				RequiresSession = true,
				DefaultMessageTimeToLive = TimeSpan.FromMinutes(5),
			};

			queueDescription = namespaceManager.CreateQueue(queueDescription);

			MessagingFactory messagingFactory = MessagingFactory.Create(
				namespaceManager.Address,
				new MessagingFactorySettings
				{
					TokenProvider = tp
				});

			queueClient = messagingFactory.CreateQueueClient(queueDescription.Path);

			Console.WriteLine("Which sample would you like to run?");
			Console.WriteLine("  1. Receive message from sessions");

			switch(Console.ReadKey(true).KeyChar)
			{
				case '1':
					Program.ReceiveMessagesFromSessions(queueClient);

					break;

				default:
					Console.WriteLine("Unknown option. Bye.");

					break;
			}
		}

		private static void ReceiveMessagesFromSessions(QueueClient queueClient)
		{
			Console.WriteLine("Receiving messages from sessions...");

			CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

			Task readSessionMessages = Task.Run(() =>
			{
				for(MessageSession nextSession = queueClient.AcceptMessageSession(TimeSpan.FromSeconds(30)); !cancellationTokenSource.IsCancellationRequested && nextSession != null; nextSession = queueClient.AcceptMessageSession(TimeSpan.FromSeconds(30)))
				{
					nextSession.AsObservable().Subscribe(
					bm =>
					{
						Console.WriteLine("Received Message: SessionId={0}, MessageId={1}", bm.SessionId, bm.MessageId);
					},
					cancellationTokenSource.Token);
				}
			},
			cancellationTokenSource.Token);

			Task writeSessionMessages = Task.Run(async () =>
				{
					int messageId = 0;
					int sessionId = 0;

					do
					{
						await queueClient.SendAsync(new BrokeredMessage
						{
							SessionId = sessionId.ToString(),
							MessageId = messageId.ToString()
						});

						sessionId++;

						if(sessionId % 4 == 0)
						{
							sessionId = 0;
						}

						messageId++;
					} while(!cancellationTokenSource.Token.IsCancellationRequested);
				},
				cancellationTokenSource.Token);

			Console.WriteLine("Press any key to stop...");
			Console.ReadKey(true);

			cancellationTokenSource.Cancel();

			Console.WriteLine("Stopping...");

			Task.WaitAll(readSessionMessages, writeSessionMessages);

			Console.WriteLine("Stopped!");
		}
	}
}
