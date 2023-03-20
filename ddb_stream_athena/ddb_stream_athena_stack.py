from aws_cdk import (
    Duration,
    Stack,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    CfnOutput as CfnOutput,
    aws_iam as iam,
    aws_glue as glue,
    aws_lambda_event_sources as event_sources
)
from constructs import Construct
import os

TABLE_NAME = "tb_sample_data"
SAMPLE_DATA_CRAWLER_NAME = "sample_data_crawler"
formatted_files_prefix = "formatted"
details_prefix = "details"
locations_prefix = "locations"
fatalities_prefix = "fatalities"
others_prefix = "others"

class DdbStreamAthenaStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)
        ddb_table = self.build_destination_table()
        source_bucket = self.build_source_bucket()
        transformer_lambda = self.build_transformer_lambda(source_bucket)
        ddb_table.grant_read_write_data(transformer_lambda)

        transformer_lambda.add_event_source(event_sources.DynamoEventSource(ddb_table,
            starting_position=lambda_.StartingPosition.LATEST,
            parallelization_factor=2,
            batch_size=10,
            enabled=True
        ))
        self.build_data_analytics(source_bucket=source_bucket)

    def build_transformer_lambda(self , source_bucket):
        transformer_lambda_role = iam.Role(self, "DecompressGZLambdaRole",
                                             assumed_by=iam.ServicePrincipal("lambda.amazonaws.com")
                                             )

        transformer_lambda_role.add_to_policy(iam.PolicyStatement(
            resources=[f"{source_bucket.bucket_arn}/*"],
            actions=["s3:*"]
        ))

        transformer_lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"))

        return lambda_.Function(
            self, 'TransformAndStoreForDataAnalytics',
            runtime=lambda_.Runtime.PYTHON_3_9,
            code=lambda_.Code.from_asset('ddb_stream_athena/transform_and_save'),
            handler='index.handler',
            role=transformer_lambda_role,
            environment={
                "raw_source_bucket": source_bucket.bucket_name
            },
            timeout=Duration.minutes(15)
    )
    def build_destination_table(self):
        return dynamodb.Table(
            self,
            "SourceDDBTable",
            partition_key=dynamodb.Attribute(
                name="id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            table_name=TABLE_NAME,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES
        )

    def build_source_bucket(self):
        return s3.Bucket(
            self,
            "DataAnalyticsBucket",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
        )

    def build_data_analytics(self, source_bucket):

        sample_data_glue_db = \
            glue.CfnDatabase(self, "SampleDataDatabase",
                             catalog_id=os.environ["CDK_DEFAULT_ACCOUNT"],
                             database_input=glue.CfnDatabase.DatabaseInputProperty(
                                 create_table_default_permissions=[
                                     glue.CfnDatabase.PrincipalPrivilegesProperty(
                                         permissions=["ALL"],
                                         principal=glue.CfnDatabase.DataLakePrincipalProperty(
                                             data_lake_principal_identifier="IAM_ALLOWED_PRINCIPALS"
                                         )
                                     )],
                                 description="Database of sample data",
                                 name="sample_data_db"
                             )
                             )

        glue_crawler_role = iam.Role(self, "GlueCrawlerRole",
                                     assumed_by=iam.ServicePrincipal("glue.amazonaws.com")
                                     )
        glue_crawler_role.add_to_policy(iam.PolicyStatement(
            resources=[f"arn:aws:s3:::{source_bucket.bucket_name}/{formatted_files_prefix}/*"],
            actions=["s3:GetObject",
                     "s3:PutObject"]
        ))
        glue_crawler_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"))

        # TODO CREATE GLUE CRAWLER FOR THE S3 BUCKET
        glue.CfnCrawler(self, "SampleDataGlueCrawler",
                        database_name=sample_data_glue_db.database_input.name,
                        name=SAMPLE_DATA_CRAWLER_NAME,
                        role=glue_crawler_role.role_arn,
                        recrawl_policy=glue.CfnCrawler.RecrawlPolicyProperty(
                            recrawl_behavior="CRAWL_NEW_FOLDERS_ONLY"
                        ),
                        schema_change_policy=glue.CfnCrawler.SchemaChangePolicyProperty(
                            delete_behavior="LOG",
                            update_behavior="LOG"
                        ),
                        targets=glue.CfnCrawler.TargetsProperty(
                            s3_targets=[
                                glue.CfnCrawler.S3TargetProperty(
                                    path=f"s3://{source_bucket.bucket_name}/{formatted_files_prefix}/{details_prefix}/",
                                ),
                                glue.CfnCrawler.S3TargetProperty(
                                    path=f"s3://{source_bucket.bucket_name}/{formatted_files_prefix}/{locations_prefix}/",
                                ),
                                glue.CfnCrawler.S3TargetProperty(
                                    path=f"s3://{source_bucket.bucket_name}/{formatted_files_prefix}/{fatalities_prefix}/",
                                ),
                                glue.CfnCrawler.S3TargetProperty(
                                    path=f"s3://{source_bucket.bucket_name}/{formatted_files_prefix}/{others_prefix}/",
                                ),
                            ]
                        ),
                        )
