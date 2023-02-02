using Amazon.Lambda.SQSEvents;
using Kralizek.Lambda;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using static Amazon.Lambda.SQSEvents.SQSBatchResponse;

namespace Kralizek.Lambda.Internal
{
    /// <summary>
    /// Gathers information about which SQS messages threw an exception, to facilitate implementing Partial Batch Response support.
    /// </summary>
    /// <seealso cref="SqsBatchResponseFunction" />
    public sealed class SqsBatchResponseProvider : IEventResponseProvider<SQSBatchResponse>
    {
        private bool _activated;
        private readonly ConcurrentBag<string> _failures = new ConcurrentBag<string>();

        /// <summary>
        /// Records an SQS Message ID as having failed during handler execution.
        /// </summary>
        /// <param name="messageId">The failing SQS Message ID.</param>
        public void RecordFailure(string messageId)
        {
            ArgumentNullException.ThrowIfNull(messageId);
            _failures.Add(messageId);
        }

        /// <inheritdoc />
        void IEventResponseProvider<SQSBatchResponse>.Activate() =>
            _activated = true;

        /// <inheritdoc />
        public void AssertActivated()
        {
            if (!_activated)
            {
                throw new InvalidOperationException($"When using {nameof(ServiceCollectionExtensions.WithReportBatchItemFailures)}, the entry point must derive from {nameof(SqsBatchResponseFunction)}");
            }
        }

        /// <inheritdoc />
        SQSBatchResponse IEventResponseProvider<SQSBatchResponse>.GetResponse()
        {
            IEnumerable<BatchItemFailure> batchItemFailures = _failures.Select(messageId => new BatchItemFailure() { ItemIdentifier = messageId });
            return new SQSBatchResponse(batchItemFailures.ToList());
        }
    }
}
