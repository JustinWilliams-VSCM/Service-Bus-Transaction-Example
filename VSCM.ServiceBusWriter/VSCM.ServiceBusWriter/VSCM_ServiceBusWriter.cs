using Microsoft.ServiceBus.Messaging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.ServiceBus;
using VSCM.MessagingProtocol;

namespace VSCM.ServiceBusWriter
{
    /// <summary>
    /// Visible Service Bus Writer
    /// </summary>
    public class VSCM_ServiceBusWriter : IDisposable
    {
        /// <summary>
        /// Write a single message, to the Visible Service Bus
        /// </summary>
        /// <param name="path">Queue/Topic name to write the message to</param>
        /// <param name="transferqueue">Queue name to use for transactions</param>
        /// <param name="message">Message to be written to the Visible Service Bus</param>
        /// <returns>List of any non accounted for errors thrown</returns>
        public List<ErrorMessage> WriteMessageToServiceBus(string path, string transferqueue, VSCM_ServiceBusMessage message)
        {
            var errors = new List<ErrorMessage>();

            using (var scope = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { Timeout = new TimeSpan(1, 0, 0, 0) }))
            {
                var settings = Settings.CreateSettings(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), path, transferqueue, message);

                var factory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);
                factory.RetryPolicy = RetryPolicy.NoRetry;

                var sender = factory.CreateMessageSender(settings.Path, settings.TransferQueue);
                sender.RetryPolicy = RetryPolicy.NoRetry;

                errors.AddRange(SynchronouslyWrite(settings, sender));

                if (!settings.ErrorMessages.IsEmpty)
                {
                    scope.Dispose();
                }
                else
                {
                    scope.Complete();
                }
            }

            if (errors.Any())
            {
                using (var scope = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { Timeout = new TimeSpan(1, 0, 0, 0) }))
                {
                    var settings = Settings.CreateSettings(ConfigurationManager.AppSettings.Get("SecondaryConnectionString"), path, transferqueue, message);

                    var factory = MessagingFactory.CreateFromConnectionString(settings.ConnectionString);
                    factory.RetryPolicy = RetryPolicy.NoRetry;

                    var sender = factory.CreateMessageSender(settings.Path, settings.TransferQueue);
                    sender.RetryPolicy = RetryPolicy.NoRetry;

                    errors.AddRange(SynchronouslyWrite(settings, sender));

                    if (!settings.ErrorMessages.IsEmpty)
                    {
                        scope.Dispose();
                    }
                    else
                    {
                        scope.Complete();
                    }
                }
            }

            return errors;
        }

        /// <summary>
        /// Write multiple messages, asynchronously, to the Visible Service Bus
        /// </summary>
        /// <param name="path">Queue/Topic name to write the message to</param>
        /// <param name="transferqueue">Queue name to use for transactions</param>
        /// <param name="messages">Message to be written to the Visible Service Bus</param>
        /// <returns>List of any non accounted for errors thrown</returns>
        public List<ErrorMessage> WriteMessagesToServiceBus(string path, string transferqueue, List<VSCM_ServiceBusMessage> messages)
        {
            var errors = new List<ErrorMessage>();

            CancellationTokenSource cancellationTokenSource;

            List<PerformanceTask> tasks;

            using (var scope = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { Timeout = new TimeSpan(1, 0, 0, 0) }))
            {
                cancellationTokenSource = new CancellationTokenSource();

                var settings = Settings.CreateBatchSettings(ConfigurationManager.AppSettings.Get("PrimaryConnectionString"), path, transferqueue, messages);

                tasks = AsynchronouslyWriteBatch(settings, cancellationTokenSource).Result;

                while (!tasks.All(t => t.Senders.All(s => s.Finished))) { }

                errors.AddRange(settings.ErrorMessages.ToList());

                //This is here to make sure all messages sent make it over the wire before the connection is severed
                Thread.Sleep(new TimeSpan(0, 0, 0, 3));

                if (settings.ErrorMessages.Any())
                {
                    scope.Dispose();
                }
                else
                {
                    scope.Complete();
                }
            }

            Stop(tasks, cancellationTokenSource);

            if (errors.Any())
            {
                using (var scope = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions { Timeout = new TimeSpan(1, 0, 0, 0) }))
                {
                    cancellationTokenSource = new CancellationTokenSource();

                    var settings = Settings.CreateBatchSettings(ConfigurationManager.AppSettings.Get("SecondaryConnectionString"), path, transferqueue, messages);

                    tasks = AsynchronouslyWriteBatch(settings, cancellationTokenSource).Result;

                    while (!tasks.All(t => t.Senders.All(s => s.Finished))) { }

                    errors.AddRange(settings.ErrorMessages.ToList());

                    //This is here to make sure all messages sent make it over the wire before the connection is severed
                    Thread.Sleep(new TimeSpan(0, 0, 0, 3));

                    if (settings.ErrorMessages.Any())
                    {
                        scope.Dispose();
                    }
                    else
                    {
                        scope.Complete();
                    }
                }

                Stop(tasks, cancellationTokenSource);
            }

            return errors;
        }

        private async void Stop(List<PerformanceTask> tasks, CancellationTokenSource cancellationTokenSource)
        {
            cancellationTokenSource.Cancel();

            await tasks.ParallelForEachAsync(async (t) => await t.CloseAsync());
        }

        private List<ErrorMessage> SynchronouslyWrite(Settings settings, MessageSender sender)
        {
            while (!settings.Messages.IsEmpty)
            {
                var message = Extensions.GetMessage(settings.Messages).Result;

                if (message != null)
                {
                    SendMessage.Send(settings, sender, message);
                }
            }

            return settings.ErrorMessages.ToList();
        }

        private async Task<List<PerformanceTask>> AsynchronouslyWriteBatch(Settings settings, CancellationTokenSource cancellationTokenSource)
        {
            var tasks = new List<PerformanceTask>
            {
                new SenderTask(settings, cancellationTokenSource.Token),
                new SenderTask(settings, cancellationTokenSource.Token),
                new SenderTask(settings, cancellationTokenSource.Token),
                new SenderTask(settings, cancellationTokenSource.Token),
                new SenderTask(settings, cancellationTokenSource.Token)
            };

            tasks.ForEach(t => t.Open());

            await tasks.ParallelForEachAsync(async (t) => await t.StartAsync());

            return tasks;
        }

#pragma warning disable 1591
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing) { }
        }
#pragma warning restore 1591
    }

    internal sealed class SendMessage
    {
        public static void Send(Settings settings, MessageSender sender, BrokeredMessage message)
        {
            try
            {
                sender.Send(message);

                if (settings.ThrowError)
                {
                    throw new Exception("test");
                }

                message.Dispose();
            }
            catch (ServerBusyException)
            {
                settings.Messages.Enqueue(message);

                Thread.Sleep(new TimeSpan(0, 0, 0, 10));
            }
            catch (Exception e)
            {
                settings.ErrorMessages.Enqueue(new ErrorMessage(settings.Path, message, e));
            }
        }

        public static async Task SendAsync(Settings settings, MessageSender sender, BrokeredMessage message)
        {
            try
            {
                await sender.SendAsync(message);

                if (settings.ThrowError)
                {
                    throw new Exception("test");
                }

                message.Dispose();
            }
            catch (ServerBusyException)
            {
                settings.Messages.Enqueue(message);

                Thread.Sleep(new TimeSpan(0, 0, 0, 10));
            }
            catch (Exception e)
            {
                var newError = new ErrorMessage(settings.Path, message, e);

                settings.ErrorMessages.Enqueue(newError);
            }
        }
    }

#pragma warning disable 1591
    public sealed class ErrorMessage
    {
        public string Destination { get; }
        public BrokeredMessage Message { get; }
        public Exception Exception { get; }

        public ErrorMessage(string destination, BrokeredMessage message, Exception exception)
        {
            Destination = destination;
            Message = message;
            Exception = exception;
        }
    }
#pragma warning restore 1591

    internal sealed class Sender
    {
        public bool Finished { get; set; }
        public MessageSender MsgSender { get; set; }
    }

    internal sealed class SenderTask : PerformanceTask
    {
        public SenderTask(Settings settings, CancellationToken cancellationToken) : base(settings, cancellationToken) { }

        protected override void OnOpen()
        {
            for (int i = 0; i < Settings.SenderCount; i++)
            {
                var factory = MessagingFactory.CreateFromConnectionString(ConnectionString);
                factory.RetryPolicy = RetryPolicy.NoRetry;
                Factories.Add(factory);

                var sender = new Sender
                {
                    Finished = false,
                    MsgSender = factory.CreateMessageSender(Settings.Path, Settings.TransferQueue)
                };

                sender.MsgSender.RetryPolicy = RetryPolicy.NoRetry;
                Senders.Add(sender);
            }
        }

        protected override async Task OnStart()
        {
            await SendTask();
        }

        async Task SendTask()
        {
            await Senders.ParallelForEachAsync(async (sender, senderIndex) =>
            {
                await ExecuteOperationAsync(async () =>
                {
                    while (!Settings.Messages.IsEmpty && !CancellationToken.IsCancellationRequested)
                    {
                        var sendingMessage = Extensions.GetMessage(Settings.Messages).Result;

                        if (sendingMessage != null)
                        {
                            await SendMessage.SendAsync(Settings, sender.MsgSender, sendingMessage);
                        }
                    }

                    sender.Finished = true;
                });
            });
        }
    }

    internal abstract class PerformanceTask
    {
        protected PerformanceTask(Settings settings, CancellationToken cancellationToken)
        {
            Settings = settings;
            Factories = new List<MessagingFactory>();
            Senders = new List<Sender>();
            CancellationToken = cancellationToken;
            ConnectionString = new ServiceBusConnectionStringBuilder(Settings.ConnectionString) { TransportType = settings.TransportType }.ToString();
        }

        protected Settings Settings { get; }

        protected string ConnectionString { get; }

        protected List<MessagingFactory> Factories { get; }

        public List<Sender> Senders { get; set; }

        protected CancellationToken CancellationToken { get; }

        public void Open()
        {
            OnOpen();
        }

        public async Task StartAsync()
        {
            await OnStart();
        }

        public async Task CloseAsync()
        {
            await Factories.ParallelForEachAsync(async (f) => await Extensions.IgnoreExceptionAsync(async () => await f.CloseAsync()));
        }

        protected abstract void OnOpen();

        protected abstract Task OnStart();

        protected async Task ExecuteOperationAsync(Func<Task> action)
        {
            TimeSpan sleep = TimeSpan.Zero;

            try
            {
                await action();
            }
            catch (ServerBusyException)
            {
                sleep = TimeSpan.FromSeconds(10);
            }

            if (sleep > TimeSpan.Zero && !CancellationToken.IsCancellationRequested)
            {
                await Extensions.Delay(sleep, CancellationToken);
            }
        }
    }

    internal static class Extensions
    {
        public static async Task ParallelForEachAsync<TSource>(this IEnumerable<TSource> source, Func<TSource, Task> action)
        {
            List<Task> tasks = new List<Task>();
            foreach (TSource i in source)
            {
                tasks.Add(action(i));
            }

            await Task.WhenAll(tasks.ToArray());
        }

        public static async Task ParallelForEachAsync<TSource>(this IEnumerable<TSource> source, Func<TSource, long, Task> action)
        {
            List<Task> tasks = new List<Task>();

            long index = 0;
            foreach (TSource i in source)
            {
                tasks.Add(action(i, index));
                index++;
            }

            await Task.WhenAll(tasks.ToArray());
        }

        public static async Task Delay(TimeSpan delay, CancellationToken cancellationToken)
        {
            try
            {
                await Task.Delay(delay, cancellationToken);
            }
            catch (TaskCanceledException)
            {

            }
        }

        public static async Task IgnoreExceptionAsync(Func<Task> task)
        {
            try
            {
                await task();
            }
            catch
            {

            }
        }

        public static async Task<BrokeredMessage> GetMessage(ConcurrentQueue<BrokeredMessage> messages)
        {
            BrokeredMessage message = null;

            if (!messages.IsEmpty)
            {
                if (!messages.TryDequeue(out message))
                {
                    message = await GetMessage(messages);
                }
            }

            return message;
        }
    }

    internal sealed class Settings
    {
        public string ConnectionString { get; set; }

        public ConnectivityMode ConnectivityMode { get; set; }

        public string Path { get; set; }

        public string TransferQueue { get; set; }

        public int SendBatchCount { get; set; }

        public int SenderCount { get; set; }

        public TransportType TransportType { get; set; }

        public ConcurrentQueue<BrokeredMessage> Messages { get; set; }

        public ConcurrentQueue<ErrorMessage> ErrorMessages { get; set; }

        public bool ThrowError { get; set; }

        public static Settings CreateSettings(string connectionString, string path, string transferqueue, VSCM_ServiceBusMessage message)
        {
            var settings = new Settings
            {
                ConnectionString = connectionString,
                Path = path,
                TransferQueue = transferqueue,
                ConnectivityMode = ConnectivityMode.Tcp,
                SenderCount = 10,
                TransportType = TransportType.NetMessaging,
                Messages = new ConcurrentQueue<BrokeredMessage>(),
                ErrorMessages = new ConcurrentQueue<ErrorMessage>()
            };

            var partitionKey = Convert.ToString(Guid.NewGuid());

            var messages = new List<VSCM_ServiceBusMessage>
            {
                message
            };

            var brokeredMessages = GetBrokeredMessages(partitionKey, messages);

            foreach (var brokeredMessage in brokeredMessages)
            {
                settings.Messages.Enqueue(brokeredMessage);
            }

            return settings;
        }

        public static Settings CreateBatchSettings(string connectionString, string path, string transferqueue, List<VSCM_ServiceBusMessage> messages)
        {
            var settings = new Settings
            {
                ConnectionString = connectionString,
                Path = path,
                TransferQueue = transferqueue,
                ConnectivityMode = ConnectivityMode.Tcp,
                SenderCount = 10,
                TransportType = TransportType.NetMessaging,
                Messages = new ConcurrentQueue<BrokeredMessage>(),
                ErrorMessages = new ConcurrentQueue<ErrorMessage>()
            };

            var partitionKey = Convert.ToString(Guid.NewGuid());

            var brokeredMessages = GetBrokeredMessages(partitionKey, messages);

            foreach (var brokeredMessage in brokeredMessages)
            {
                settings.Messages.Enqueue(brokeredMessage);
            }

            return settings;
        }

        private static List<BrokeredMessage> GetBrokeredMessages(string partitionKey, List<VSCM_ServiceBusMessage> messages)
        {
            var brokeredMessages = new List<BrokeredMessage>();

            foreach (var message in messages)
            {
                var tempMsg = new BrokeredMessage(message)
                {
                    MessageId = Convert.ToString(Guid.NewGuid()),
                    PartitionKey = partitionKey,
                    ViaPartitionKey = partitionKey
                };

                brokeredMessages.Add(tempMsg);
            }

            return brokeredMessages;
        }
    }
}