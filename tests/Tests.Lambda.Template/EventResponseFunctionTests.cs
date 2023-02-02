using Amazon.Lambda.Core;
using Amazon.Lambda.TestUtilities;
using Kralizek.Lambda;
using Kralizek.Lambda.Internal;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace Tests.Lambda
{
    [TestFixture]
    public class EventResponseFunctionTests
    {
        private TestEventResponseFunction CreateSystemUnderTest()
        {
            return new TestEventResponseFunction();
        }

        [Test]
        public void Configure_should_be_invoked_on_type_initialization()
        {
            var sut = CreateSystemUnderTest();

            Assert.True(sut.IsConfigureInvoked);
        }

        [Test]
        public void ConfigureServices_should_be_invoked_on_type_initialization()
        {
            var sut = CreateSystemUnderTest();

            Assert.True(sut.IsConfigureServicesInvoked);
        }

        [Test]
        public async Task Function_runs()
        {
            var sut = new TestEventResponseFunction();

            var result = await sut.FunctionHandlerAsync("Hi there", new TestLambdaContext());
            Assert.That(result, Is.EqualTo("Hi there"));
        }

        public class TestEventResponseFunction : EventResponseFunction<string, string>
        {
            protected override void Configure(IConfigurationBuilder builder) => IsConfigureInvoked = true;

            protected override void ConfigureServices(IServiceCollection services, IExecutionEnvironment executionEnvironment)
            {
                services.AddScoped<IEventHandler<string>, TestHandler>();
                services.AddScoped<TestResponseProvider>();
                services.AddScoped<IEventResponseProvider<string>>(isp => isp.GetRequiredService<TestResponseProvider>());

                IsConfigureServicesInvoked = true;
            }

            protected override void ConfigureLogging(ILoggingBuilder loggerFactory, IExecutionEnvironment executionEnvironment) => IsConfigureLoggingInvoked = true;

            public bool IsConfigureInvoked { get; private set; }

            public bool IsConfigureServicesInvoked { get; private set; }

            public bool IsConfigureLoggingInvoked { get; private set; }
        }

        private class TestHandler : IEventHandler<string>
        {
            private readonly IServiceProvider _serviceProvider;

            public TestHandler(IServiceProvider serviceProvider)
            {
                _serviceProvider = serviceProvider ?? throw new ArgumentNullException(nameof(serviceProvider));
            }

            public Task HandleAsync(string input, ILambdaContext context)
            {
                var responseProvider = _serviceProvider.GetService<TestResponseProvider>();
                responseProvider.AssertActivated();

                responseProvider.IngestString(input);

                return Task.CompletedTask;
            }
        }
        private class TestResponseProvider : IEventResponseProvider<string>
        {
            private readonly ConcurrentBag<string> _strings = new ConcurrentBag<string>();
            private bool _activated;

            public void IngestString(string str) =>
                _strings.Add(str);

            void IEventResponseProvider<string>.Activate() =>
                _activated = true;

            public void AssertActivated()
            {
                if (!_activated)
                {
                    throw new InvalidOperationException($"{nameof(TestResponseProvider)} not activated");
                }
            }

            public string GetResponse() =>
                string.Join(';', _strings.OrderBy(x => x));
        }
    }
}
