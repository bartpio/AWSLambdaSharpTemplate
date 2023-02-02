using Amazon.Lambda.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;

namespace Kralizek.Lambda.Internal;

/// <summary>
/// A base class used for Event Functions that don't return a response directly from their handler,
/// but do use a batch-scoped <see cref="IEventResponseProvider{TInput}" /> after handler execution
/// to gather a Lambda response. In particular, this is useful as part of implementing Partial Batch Responses,
/// where we catch handler errors, and tell AWS which messages failed to process.
/// </summary>
/// <typeparam name="TInput">The type of the incoming request.</typeparam>
/// <typeparam name="TOutput">The type of the outgoing response.</typeparam>
/// <seealso href="https://aws.amazon.com/about-aws/whats-new/2021/11/aws-lambda-partial-batch-response-sqs-event-source/" />
public abstract class EventResponseFunction<TInput, TOutput> : Function
{
    /// <summary>
    /// The entrypoint used by the Lambda runtime for executing the function. 
    /// </summary>
    /// <param name="input">The incoming request.</param>
    /// <param name="context">A representation of the execution context.</param>
    /// <exception cref="InvalidOperationException">The exception is thrown if no handler is registered for the incoming input,
    /// or if no handler is registered for gathering output.</exception>
    public async Task<TOutput> FunctionHandlerAsync(TInput input, ILambdaContext context)
    {
        using var scope = ServiceProvider.CreateScope();

        var handler = scope.ServiceProvider.GetService<IEventHandler<TInput>>();

        if (handler == null)
        {
            Logger.LogCritical("No {Handler} could be found", $"IEventHandler<{typeof(TInput).Name}>");
            throw new InvalidOperationException($"No IEventHandler<{typeof(TInput).Name}> could be found.");
        }

        var responseHandler = scope.ServiceProvider.GetService<IEventResponseProvider<TOutput>>();

        if (responseHandler == null)
        {
            Logger.LogCritical("No {ResponseHandler} could be found", $"IEventResponseHandler<{typeof(TOutput).Name}>");
            throw new InvalidOperationException($"No IEventResponseHandler<{typeof(TOutput).Name}> could be found.");
        }

        responseHandler.Activate();

        Logger.LogInformation("Invoking handler");
        await handler.HandleAsync(input, context).ConfigureAwait(false);
        return responseHandler.GetResponse();
    }

    /// <summary>
    /// Registers the handler for the request ot type <typeparamref name="TInput"/>.
    /// </summary>
    /// <param name="services">The collections of services.</param>
    /// <param name="lifetime">The lifetime of the handler. Defaults to <see cref="ServiceLifetime.Transient"/>.</param>
    /// <typeparam name="THandler">The type of the handler for requests of type <typeparamref name="TInput"/>.</typeparam>
    protected void RegisterHandler<THandler>(IServiceCollection services, ServiceLifetime lifetime = ServiceLifetime.Transient) where THandler : class, IEventHandler<TInput>
    {
        services.Add(ServiceDescriptor.Describe(typeof(IEventHandler<TInput>), typeof(THandler), lifetime));
    }
}

/// <summary>
/// An interface that describes a provider for gathering outputs of the type <typeparamref name="TOutput"/>.
/// </summary>
/// <typeparam name="TOutput">The type of the outgoing response.</typeparam>
public interface IEventResponseProvider<TOutput>
{
    /// <summary>
    /// Registers the fact that an appropriate <see cref="Function" /> (generally <see cref="EventResponseFunction{TInput,TOutput}" />) is in use,
    /// and will provide the response gathered by the provider as the Lambda response.
    /// </summary>
    void Activate();

    /// <summary>
    /// Verifies that an appropriate an appropriate <see cref="Function" /> has announced itself by calling <see cref="Activate" />.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown in the event that an appropriate <see cref="Function" /> has not announced itself.</exception>
    void AssertActivated();

    /// <summary>
    /// Get the response that has accumulated after <see cref="IEventHandler{TInput}" /> executions.
    /// </summary>
    /// <returns>The accumulated response for the entire batch, that will be returned from the Lambda.</returns>
    TOutput GetResponse();
}