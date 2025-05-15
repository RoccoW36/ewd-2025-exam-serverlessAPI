import { marshall, unmarshall } from "@aws-sdk/util-dynamodb";
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { CinemaSchedule } from "./types";

type Entity = CinemaSchedule; 

// Helper to create a DynamoDB Document Client
export const createDDbDocClient = () => {
  const ddbClient = new DynamoDBClient({ region: process.env.REGION });
  return DynamoDBDocumentClient.from(ddbClient);
};

// Helper to generate a PutRequest item (Used in Batch Writes)
export const generateItem = (entity: Entity) => {
  return {
    PutRequest: {
      Item: marshall(entity),
    },
  };
};

// Helper to generate Batch Write Items
export const generateBatch = (data: Entity[]) => {
  return data.map((e) => generateItem(e));
};

// Helper to generate Update Parameters
export const generateUpdateParams = (entity: Partial<Entity>) => {
  const updateExpression = [];
  const expressionAttributeValues: Record<string, any> = {};

  if (entity.movieTitle) {
    updateExpression.push("movieTitle = :movieTitle");
    expressionAttributeValues[":movieTitle"] = entity.movieTitle;
  }
  if (entity.period) {
    updateExpression.push("period = :period");
    expressionAttributeValues[":period"] = entity.period;
  }
  if (entity.city) {
    updateExpression.push("city = :city");
    expressionAttributeValues[":city"] = entity.city;
  }

  return {
    UpdateExpression: `SET ${updateExpression.join(", ")}`,
    ExpressionAttributeValues: marshall(expressionAttributeValues),
  };
};

// Helper to parse DynamoDB response into JavaScript objects
export const parseDynamoDbItem = (data: any) => {
  return unmarshall(data);
};
