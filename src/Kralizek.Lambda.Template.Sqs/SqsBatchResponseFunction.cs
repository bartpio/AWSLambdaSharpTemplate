﻿using Amazon.Lambda.SQSEvents;
using Kralizek.Lambda.Internal;

namespace Kralizek.Lambda
{
    /// <summary>
    /// A base class that can be used for SQS Event Functions for which Lambda Partial Batch Responses are enabled.
    /// Partial Batch Responses inform Lambda of which messages resulted in errors (exceptions thrown during handling),
    /// so that the entire batch doesn't need to be retried.
    /// Partial Batch Responses are enabled by setting the ReportBatchItemFailures response type on the Lambda.
    /// Use with <see cref="ServiceCollectionExtensions.WithReportBatchItemFailures{TMessage}(IMessageHandlerConfigurator{TMessage})" />.
    /// </summary>
    /// <seealso href="https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/aws-resource-lambda-eventsourcemapping.html#cfn-lambda-eventsourcemapping-functionresponsetypes" />
    /// <seealso href="https://aws.amazon.com/about-aws/whats-new/2021/11/aws-lambda-partial-batch-response-sqs-event-source/" />
    /// <seealso href="https://docs.aws.amazon.com/lambda/latest/dg/with-sqs.html" />
    public abstract class SqsBatchResponseFunction : EventResponseFunction<SQSEvent, SQSBatchResponse>
    {
    }
}
