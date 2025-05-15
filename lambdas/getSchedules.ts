import { APIGatewayProxyHandlerV2 } from "aws-lambda";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBDocumentClient,
  QueryCommand,
  ScanCommand,
  QueryCommandInput,
} from "@aws-sdk/lib-dynamodb";
import Ajv from "ajv";
import schema from "../shared/types.schema.json";

function createDDbDocClient() {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  const marshallOptions = {
    convertEmptyValues: true,
    removeUndefinedValues: true,
    convertClassInstanceToMap: true,
  };
  const unmarshallOptions = {
    wrapNumbers: false,
  };
  const translateConfig = { marshallOptions, unmarshallOptions };
  return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}

const client = createDDbDocClient();

const ajv = new Ajv();
const validateCinemaId = ajv.compile(schema.definitions.CinemaScheduleQueryParams);
const validateMovieId = ajv.compile({
  type: "object",
  properties: { movieId: { type: "string" } },
  required: ["movieId"],
  additionalProperties: false,
});
const validatePeriod = ajv.compile({
  type: "object",
  properties: { period: { type: "string" } },
  additionalProperties: false,
});

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
  try {
    console.log("Event: ", JSON.stringify(event));

    const cinemaId = event.queryStringParameters?.cinemaId;
    const movieId = event.queryStringParameters?.movieId;
    const period = event.queryStringParameters?.period;

    if (cinemaId && !validateCinemaId({ cinemaId: Number(cinemaId) })) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid 'cinemaId': must be a number." }),
      };
    }

    if (movieId && !validateMovieId({ movieId })) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid 'movieId': must be a string." }),
      };
    }

    if (period && !validatePeriod({ period })) {
      return {
        statusCode: 400,
        body: JSON.stringify({ error: "Invalid 'period': must be a string." }),
      };
    }

    // Query by cinemaId and period
    if (cinemaId && period) {
      const queryParams: QueryCommandInput = {
        TableName: "CinemaTable",
        KeyConditionExpression: "cinemaId = :cinemaId AND period = :period",
        ExpressionAttributeValues: {
          ":cinemaId": Number(cinemaId),
          ":period": period,
        },
      };
      const result = await client.send(new QueryCommand(queryParams));
      return {
        statusCode: 200,
        body: JSON.stringify(result.Items || []),
      };
    }

    // Query by cinemaId and movieId
    if (cinemaId && movieId) {
      const queryParams: QueryCommandInput = {
        TableName: "CinemaTable",
        KeyConditionExpression: "cinemaId = :cinemaId AND movieId = :movieId",
        ExpressionAttributeValues: {
          ":cinemaId": Number(cinemaId),
          ":movieId": movieId,
        },
      };
      const result = await client.send(new QueryCommand(queryParams));
      return {
        statusCode: 200,
        body: JSON.stringify(result.Items || []),
      };
    }

    // Query by cinemaId only
    if (cinemaId) {
      const queryParams: QueryCommandInput = {
        TableName: "CinemaTable",
        KeyConditionExpression: "cinemaId = :cinemaId",
        ExpressionAttributeValues: {
          ":cinemaId": Number(cinemaId),
        },
      };
      const result = await client.send(new QueryCommand(queryParams));
      return {
        statusCode: 200,
        body: JSON.stringify(result.Items || []),
      };
    }

    // Query by movieId only
    if (movieId) {
      const scanParams = {
        TableName: "CinemaTable",
        FilterExpression: "movieId = :movieId",
        ExpressionAttributeValues: {
          ":movieId": movieId,
        },
        Limit: 50,
      };
      const result = await client.send(new ScanCommand(scanParams));
      return {
        statusCode: 200,
        body: JSON.stringify(result.Items || []),
      };
    }

    // Query by period only
    if (period) {
      const scanParams = {
        TableName: "CinemaTable",
        FilterExpression: "period = :period",
        ExpressionAttributeValues: {
          ":period": period,
        },
        Limit: 50,
      };
      const result = await client.send(new ScanCommand(scanParams));
      return {
        statusCode: 200,
        body: JSON.stringify(result.Items || []),
      };
    }

    // No params, return full scan
    const scanParams = {
      TableName: "CinemaTable",
      Limit: 100,
    };
    const result = await client.send(new ScanCommand(scanParams));
    return {
      statusCode: 200,
      body: JSON.stringify(result.Items || []),
    };

  } catch (error: any) {
    console.error("Error querying DynamoDB:", JSON.stringify(error));
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Internal Server Error" }),
    };
  }
};
