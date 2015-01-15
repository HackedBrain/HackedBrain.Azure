using System;
using System.IO;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace HackedBrain.Azure.ServiceBus.Tests
{
    internal class AzureServiceBusTestUtilities
    {
        private JObject serviceBusAccountInfo;
        private TokenProvider tokenProvider;
        private NamespaceManager namespaceManager;
        private MessagingFactory messagingFactory;

        private AzureServiceBusTestUtilities()
        {
            this.serviceBusAccountInfo = this.LoadAzureServiceBusAccountInfo();
        }

        public TokenProvider TokenProvider
        {
            get
            {
                if(this.tokenProvider == null)
                {
                    JObject sharedAccessSignatureToken = serviceBusAccountInfo.Value<JObject>("sharedAccessSignatureToken");
                    
                    this.tokenProvider = TokenProvider.CreateSharedAccessSignatureTokenProvider(sharedAccessSignatureToken.Value<string>("keyName"), sharedAccessSignatureToken.Value<string>("key"));
                }

                return this.tokenProvider;
            }
        }


        public NamespaceManager NamespaceManager
        {
            get
            {
                if(this.namespaceManager == null)
                {
                    this.namespaceManager = new NamespaceManager(
                        this.serviceBusAccountInfo.Value<string>("namespaceAddress"),
                        new NamespaceManagerSettings
                        {
                            TokenProvider = this.TokenProvider
                        });
                }

                return this.namespaceManager;
            }
        }

        public MessagingFactory MessagingFactory
        {
            get
            {
                if(this.messagingFactory == null)
                {
                    this.messagingFactory = MessagingFactory.Create(this.NamespaceManager.Address,
                                                                    new MessagingFactorySettings
                                                                    {
                                                                        TokenProvider = this.TokenProvider
                                                                    });
                }

                return this.messagingFactory;
            }
        }

        public static AzureServiceBusTestUtilities Create()
        {
            return new AzureServiceBusTestUtilities();
        }

        internal JObject LoadAzureServiceBusAccountInfo()
        {
            try
            {
                using(StreamReader reader = new StreamReader(@"..\..\TestAzureServiceBusAccountInfo.json"))
                using(JsonReader jsonReader = new JsonTextReader(reader))
                {
                    return JsonSerializer.Create().Deserialize<JObject>(jsonReader);
                }
            }
            catch(FileNotFoundException exception)
            {
                throw new InvalidOperationException("TestAzureServiceBusAccountInfo.json not found. Please create this file in the project's root directory and populate with your own Azure Service Bus account information.", exception);
            }
        }
    }
}
