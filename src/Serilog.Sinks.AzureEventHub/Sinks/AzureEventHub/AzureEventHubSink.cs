﻿// Copyright 2014 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.EventHubs;
using Serilog.Core;
using Serilog.Events;
using Serilog.Formatting;

namespace Serilog.Sinks.AzureEventHub
{
    /// <summary>
    /// Writes log events to an Azure Event Hub.
    /// </summary>
    public class AzureEventHubSink : ILogEventSink
    {
        readonly EventHubClient _eventHubClient;
        readonly ITextFormatter _formatter;

        /// <summary>
        /// Construct a sink that saves log events to the specified EventHubClient.
        /// </summary>
        /// <param name="eventHubClient">The EventHubClient to use in this sink.</param>
        /// <param name="formatter">Provides formatting for outputting log data</param>
        public AzureEventHubSink(
            EventHubClient eventHubClient,
            ITextFormatter formatter)
        {
            _eventHubClient = eventHubClient;
            _formatter = formatter;
        }

        /// <summary>
        /// Emit the provided log event to the sink.
        /// </summary>
        /// <param name="logEvent">The log event to write.</param>
        public void Emit(LogEvent logEvent)
        {
            byte[] body;
            using (var render = new StringWriter())
            {
                _formatter.Format(logEvent, render);
                body = Encoding.UTF8.GetBytes(render.ToString());
            }
            var eventHubData = new EventData(body);
            eventHubData.Properties.Add("Type", "SerilogEvent");
            eventHubData.Properties.Add("Level", logEvent.Level.ToString());

            //Unfortunately no support for async in Serilog yet
            //https://github.com/serilog/serilog/issues/134
            SendAsync(eventHubData).GetAwaiter().GetResult();
        }

        async Task SendAsync(EventData eventHubData)
        {
            const byte retryCount = 3;
            var partitionKey = Guid.NewGuid().ToString();

            for (var retryAttempt = 0; retryAttempt <= retryCount; retryAttempt++)
            {
                try
                {
                    await _eventHubClient.SendAsync(eventHubData, partitionKey);

                    return;
                }
                catch (EventHubsException e) when (e.IsTransient)
                {
                    await Task.Delay(TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)));

                    if (retryAttempt == retryCount)
                        throw;
                }
            }
        }
    }
}