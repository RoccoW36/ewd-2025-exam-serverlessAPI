import * as cdk from "aws-cdk-lib";
import * as lambdanode from "aws-cdk-lib/aws-lambda-nodejs";
import * as lambda from "aws-cdk-lib/aws-lambda";
import * as dynamodb from "aws-cdk-lib/aws-dynamodb";
import * as custom from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";
import { generateBatch } from "../shared/util";
import { schedules } from "../seed/movies";
import * as apig from "aws-cdk-lib/aws-apigateway";

export class ExamStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const table = new dynamodb.Table(this, "CinemasTable", {
      billingMode: dynamodb.BillingMode.PAY_PER_REQUEST,
      partitionKey: { name: "cinemaId", type: dynamodb.AttributeType.NUMBER },
      sortKey: { name: "movieId", type: dynamodb.AttributeType.STRING },
      removalPolicy: cdk.RemovalPolicy.DESTROY,
      tableName: "CinemaTable",
 });

    table.addLocalSecondaryIndex({
      indexName: "periodIx",
      sortKey: { name: "period", type: dynamodb.AttributeType.STRING },
 });

    const getCinemaSchedulesFn = new lambdanode.NodejsFunction(this, "GetCinemaSchedulesFn", {
      architecture: lambda.Architecture.ARM_64,
      runtime: lambda.Runtime.NODEJS_22_X,
      entry: `${__dirname}/../lambdas/getSchedules.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: { REGION: "eu-west-1", TABLE_NAME: table.tableName },
    });

    const question1Fn = new lambdanode.NodejsFunction(this, "QuestionFn", {
      architecture: lambda.Architecture.ARM_64,
      runtime: lambda.Runtime.NODEJS_22_X,
      entry: `${__dirname}/../lambdas/question.ts`,
      timeout: cdk.Duration.seconds(10),
      memorySize: 128,
      environment: {
        REGION: "eu-west-1",
      },
    });

    table.grantReadData(getCinemaSchedulesFn);
   
    new custom.AwsCustomResource(this, "moviesddbInitData", {
      onCreate: {
        service: "DynamoDB",
        action: "batchWriteItem",
        parameters: {
          RequestItems: {
            [table.tableName]: generateBatch(schedules),
          },
        },
        physicalResourceId: custom.PhysicalResourceId.of("moviesddbInitData"),
      },
      policy: custom.AwsCustomResourcePolicy.fromSdkCalls({
        resources: [table.tableArn],
      }),
    });

    const api = new apig.RestApi(this, "ExamAPI", {
      description: "Exam api",
      deployOptions: {
        stageName: "dev",
      },
      defaultCorsPreflightOptions: {
        allowHeaders: ["Content-Type", "X-Amz-Date"],
        allowMethods: ["OPTIONS", "GET", "POST", "PUT", "PATCH", "DELETE"],
        allowCredentials: true,
        allowOrigins: ["*"],
      },
    });

     const schedulesResource = api.root.addResource("schedules");
    schedulesResource.addMethod("GET", new apig.LambdaIntegration(getCinemaSchedulesFn));

  }
}
