import aws_cdk as core
import aws_cdk.assertions as assertions

from ddb_stream_athena.ddb_stream_athena_stack import DdbStreamAthenaStack

# example tests. To run these tests, uncomment this file along with the example
# resource in ddb_stream_athena/ddb_stream_athena_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DdbStreamAthenaStack(app, "ddb-stream-athena")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
