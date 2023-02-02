using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Kralizek.Lambda.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Kralizek.Lambda;

/// <summary>
/// An implementation of <see cref="IEventHandler{TInput}"/> specialized for <see cref="SQSEvent"/> that processes all the records in sequence.
/// </summary>
/// <typeparam name="TMessage">The internal type of the SQS message.</typeparam>
public class SqsEventHandler<TMessage> : IEventHandler<SQSEvent> where TMessage : class
{
    private readonly ILogger _logger;
    private readonly IServiceProvider _serviceProvider;

    public SqsEventHandler(IServiceProvider serviceProvider, ILoggerFactory loggerFactory)
    {
        _logger = loggerFactory?.CreateLogger("SqsEventHandler") ?? throw new ArgumentNullException(nameof(loggerFactory));
        _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
    }

    /// <summary>
    /// Handles the <see cref="SQSEvent"/> by processing each record in sequence.
    /// </summary>
    /// <param name="input">The incoming event.</param>
    /// <param name="context">The execution context.</param>
    /// <exception cref="InvalidOperationException">Thrown if there is no registered implementation of <see cref="IMessageHandler{TMessage}"/>.</exception>
    public async Task HandleAsync(SQSEvent? input, ILambdaContext context)
    {
        if (input is { Records: { } })
        {
            var batchResponseProvider = _serviceProvider.GetService<SqsBatchResponseProvider>();
            batchResponseProvider?.AssertActivated();

            foreach (var record in input.Records)
            {
                using var scope = _serviceProvider.CreateScope();

                var sqsMessage = record.Body;

                var serializer = _serviceProvider.GetRequiredService<IMessageSerializer>();

                var message = serializer.Deserialize<TMessage>(sqsMessage);

                var handler = scope.ServiceProvider.GetService<IMessageHandler<TMessage>>();

                if (handler == null)
                {
                    _logger.LogError("No {Handler} could be found", $"IMessageHandler<{typeof(TMessage).Name}>");

                    throw new InvalidOperationException($"No IMessageHandler<{typeof(TMessage).Name}> could be found.");
                }

                _logger.LogInformation("Invoking notification handler");
                try
                {
                    await handler.HandleAsync(message, context).ConfigureAwait(false);
                }
                catch (Exception exc) when (batchResponseProvider is not null)
                {
                    _logger.LogError(exc, "Recording batch item failure for message {MessageId}", record.MessageId);
                    batchResponseProvider.RecordFailure(record.MessageId);
                }
            }
        }
    }
}