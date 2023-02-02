using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.Lambda.TestUtilities;
using Kralizek.Lambda;
using Kralizek.Lambda.Internal;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Moq;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.Lambda.Sqs;

[TestFixture]
public class ParallelSqsEventHandlerTests
{
    private Mock<IMessageSerializer> _mockMessageSerializer;
    private Mock<IMessageHandler<TestMessage>> _mockMessageHandler;
    private Mock<IServiceScopeFactory> _mockServiceScopeFactory;
    private Mock<IServiceProvider> _mockServiceProvider;
    private Mock<ILoggerFactory> _mockLoggerFactory;
    private Mock<IServiceScope> _mockServiceScope;
    private ParallelSqsExecutionOptions _parallelExecutionOptions;

    [SetUp]
    public void Initialize()
    {
        _mockMessageSerializer = new Mock<IMessageSerializer>();

        _mockMessageSerializer
            .Setup(p => p.Deserialize<TestMessage>(It.IsAny<string>()))
            .Returns(() => new TestMessage());
            
        _mockMessageHandler = new Mock<IMessageHandler<TestMessage>>();
        _mockMessageHandler.Setup(p => p.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<ILambdaContext>())).Returns(Task.CompletedTask);

        _mockServiceScope = new Mock<IServiceScope>();

        _mockServiceScopeFactory = new Mock<IServiceScopeFactory>();

        _mockServiceScopeFactory.Setup(p => p.CreateScope()).Returns(_mockServiceScope.Object);

        _mockServiceProvider = new Mock<IServiceProvider>();
        _mockServiceProvider.Setup(p => p.GetService(typeof(IMessageHandler<TestMessage>)))
            .Returns(_mockMessageHandler.Object);
        _mockServiceProvider.Setup(p => p.GetService(typeof(IServiceScopeFactory)))
            .Returns(_mockServiceScopeFactory.Object);

        _mockServiceProvider
            .Setup(p => p.GetService(typeof(IMessageSerializer)))
            .Returns(_mockMessageSerializer.Object);

        _mockServiceScope.Setup(p => p.ServiceProvider).Returns(_mockServiceProvider.Object);

        _mockLoggerFactory = new Mock<ILoggerFactory>();
        _mockLoggerFactory.Setup(p => p.CreateLogger(It.IsAny<string>()))
            .Returns(Mock.Of<ILogger>());

        _parallelExecutionOptions = new ParallelSqsExecutionOptions { MaxDegreeOfParallelism = 4 };
    }

    private ParallelSqsEventHandler<TestMessage> CreateSystemUnderTest()
    {
        return new ParallelSqsEventHandler<TestMessage>(_mockServiceProvider.Object, _mockLoggerFactory.Object, Options.Create(_parallelExecutionOptions));
    }

    [Test]
    public async Task HandleAsync_resolves_MessageHandler_for_each_record()
    {
        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
            }
        };

        var lambdaContext = new TestLambdaContext();

        var sut = CreateSystemUnderTest();

        await sut.HandleAsync(sqsEvent, lambdaContext);

        _mockServiceProvider.Verify(p => p.GetService(typeof(IMessageHandler<TestMessage>)), Times.Exactly(sqsEvent.Records.Count));
    }

    [Test]
    public async Task HandleAsync_creates_a_scope_for_each_record()
    {
        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
            }
        };

        var lambdaContext = new TestLambdaContext();

        var sut = CreateSystemUnderTest();

        await sut.HandleAsync(sqsEvent, lambdaContext);

        _mockServiceScopeFactory.Verify(p => p.CreateScope(), Times.Exactly(sqsEvent.Records.Count));
    }

    [Theory]
    public async Task HandleAsync_records_exceptions_when_applicable(bool applicable)
    {
        _mockMessageHandler = new Mock<IMessageHandler<TestMessage>>();
        _mockMessageHandler.Setup(p => p.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<ILambdaContext>()))
            .Returns(applicable ? Task.FromException(new InvalidDataException()) : Task.CompletedTask);

        _mockServiceProvider.Setup(p => p.GetService(typeof(IMessageHandler<TestMessage>)))
           .Returns(_mockMessageHandler.Object);

        var batchResponseProvider = new SqsBatchResponseProvider();
        // SqsBatchResponseFunction (via its base EventResponseFunction) would normally do this
        ((IEventResponseProvider<SQSBatchResponse>)batchResponseProvider).Activate();

        _mockServiceProvider.Setup(p => p.GetService(typeof(SqsBatchResponseProvider)))
            .Returns(batchResponseProvider);

        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}",
                    MessageId = "msg1"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}",
                    MessageId = "msg2"
                },
            }
        };

        var lambdaContext = new TestLambdaContext();

        var sut = CreateSystemUnderTest();

        await sut.HandleAsync(sqsEvent, lambdaContext);

        _mockMessageHandler.Verify(p => p.HandleAsync(It.IsAny<TestMessage>(), lambdaContext), Times.Exactly(sqsEvent.Records.Count));
        var expectation = applicable ? new string[] { "msg1", "msg2" } : Array.Empty<string>();
        Assert.That(((IEventResponseProvider<SQSBatchResponse>)batchResponseProvider).GetResponse().BatchItemFailures.Select(x => x.ItemIdentifier), Is.EquivalentTo(expectation));
    }

    [Test]
    public void HandleAsync_throws_InvalidOperation_if_NotificationHandler_is_not_registered()
    {
        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
            }
        };

        var lambdaContext = new TestLambdaContext();

        _mockServiceProvider = new Mock<IServiceProvider>();
        _mockServiceProvider.Setup(p => p.GetService(typeof(IServiceScopeFactory))).Returns(_mockServiceScopeFactory.Object);

        _mockServiceScope.Setup(p => p.ServiceProvider).Returns(_mockServiceProvider.Object);

        var sut = CreateSystemUnderTest();

        Assert.ThrowsAsync<InvalidOperationException>(() => sut.HandleAsync(sqsEvent, lambdaContext));
    }

    [Test]
    public async Task MaxDegreeOfParallelism_Should_ProperlyPropagated()
    {
        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
            }
        };

        var cq = new ConcurrentQueue<Task>();

        _parallelExecutionOptions = new ParallelSqsExecutionOptions {MaxDegreeOfParallelism = 2};
        _mockMessageHandler.Setup(p => p.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<ILambdaContext>()))
            .Returns(async ()=>
            {
                var t = Task.Delay(1);
                cq.Enqueue(t);
                if (cq.Count > 2)
                {
                    throw new Exception("not good");
                }
                await t;
                cq.TryDequeue(out t);
            });

        var sut = CreateSystemUnderTest();

        await sut.HandleAsync(sqsEvent, new TestLambdaContext());

        _mockMessageHandler.VerifyAll();
        _mockMessageHandler.Verify(
            handler => handler.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<ILambdaContext>()),
            Times.Exactly(sqsEvent.Records.Count));
    }

    [Test]
    public void MaxDegreeOfParallelism_Should_ProperlyPropagated_And_Limited_To_Set_Max()
    {
        var sqsEvent = new SQSEvent
        {
            Records = new List<SQSEvent.SQSMessage>
            {
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
                new SQSEvent.SQSMessage
                {
                    Body = "{}"
                },
            }
        };

        var cq = new ConcurrentQueue<Task>();
            
        //We are checking if parallelism actually does what it's supposed to do. So we should have more then 2 concurrent processes running
        _parallelExecutionOptions = new ParallelSqsExecutionOptions { MaxDegreeOfParallelism = 4 };
        _mockMessageHandler.Setup(p => p.HandleAsync(It.IsAny<TestMessage>(), It.IsAny<ILambdaContext>()))
            .Returns(async () =>
            {
                var t = Task.Delay(1);
                cq.Enqueue(t);
                Console.WriteLine(cq.Count);
                if (cq.Count > 2)
                {
                    throw new Exception("Concurrent Tasks exceeded 2");
                }
                await t;
                cq.TryDequeue(out t);
            });
            
        var sut = CreateSystemUnderTest();

        Assert.ThrowsAsync<Exception>(() => sut.HandleAsync(sqsEvent, new TestLambdaContext()));
    }
}