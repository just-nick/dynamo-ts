import { TABLE_DEFINITION, AUTO_GENERATE } from './constants';
import * as AWS from 'aws-sdk';
import * as shortid from 'shortid';
import 'reflect-metadata';

export namespace DynamodbService {
    let dynamodb: AWS.DynamoDB;
    let dynamoDbTables: any = {};
    let migrated = false;

    const defaultTableDefinition = {
        TableName: '',
        KeySchema: [],
        AttributeDefinitions: [],
        ProvisionedThroughput: {
            ReadCapacityUnits: 10,
            WriteCapacityUnits: 10
        }
    };

    export function getDefaultTableDefinition() {
        return {...defaultTableDefinition};
    }

    export function configureDynamoDb(options: any = {}): void {
        dynamodb = new AWS.DynamoDB(config(options));
    }

    // @todo types
    export function migrate(): Promise<any> {
        let newTables: Promise<any>[] = [];

        if (migrated) {
            return Promise.resolve();
        } else {
            for (let key of Object.keys(dynamoDbTables)) {
                let tableDefinition = dynamoDbTables[key].definition;

                newTables.push(new Promise((resolve, reject) => {
                    dynamodb.createTable(tableDefinition, (err, data) => {
                        if (err) {
                            if (err.name === 'ResourceInUseException') {
                                console.log('Table exists');
                                // Table exists
                                resolve(data);
                            }
                            else {
                                reject(err);
                            }
                        } else {
                            console.log('Successfully created', tableDefinition.TableName);
                            resolve(data);
                        }
                    });
                }));
            }

            return Promise.all(newTables);
        }
    }

    export function addTable(Table: Function, definition: any, autoGenerate) {
        console.log('Add', Table.name);
        dynamoDbTables[Table.name] = {
            definition,
            autoGenerate
        };
    }

    export function get<C>(Table: TableClass<C>, Key: any): Promise<C> {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const TableName = Table.name;
        let request = {
            TableName,
            Key
        };

        return new Promise((resolve, reject) => {
            docClient.get(request, (err, data) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(data.Item as C);
                }
            })
        });
    }


    export function put<T>(Table: TableClass<T>, Item: T): Promise<T> {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const autoGeneratedKey = dynamoDbTables[Table.name].autoGenerate;

        const TableName = Table.name;

        if (autoGeneratedKey && !Item[autoGeneratedKey]) {
            Item[autoGeneratedKey] = shortid.generate();
        }

        console.log('Put', Item);

        return new Promise((resolve, reject) => {
            docClient.put({
                TableName,
                Item
            }, (err) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(Item);
                }
            })
        });
    }

    export function del<C>(Table: TableClass<C>, properties: DeleteProperties) {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const TableName = Table.name;
        const params = {
            ...properties,
            TableName: Table.name
        };

        return new Promise((resolve, reject) => {
            docClient.delete(params, (err, data) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(data);
                }
            })
        });
    }

    export function update<C>(Table: TableClass<C>, properties: UpdateProperties) {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const TableName = Table.name;
        const params = {
            ...properties,
            TableName: Table.name
        };

        return new Promise((resolve, reject) => {
            docClient.delete(params, (err, data) => {
                if (err) {
                    reject(err);
                }
                else {
                    resolve(data);
                }
            })
        });
    }

    export function scan<C>(Table: TableClass<C>, properties: ScanProperties) {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const TableName = Table.name;
        const params = {
            ...properties,
            TableName: Table.name,
            Limit: 100
        };

        console.log('Scan as', params);

        return new Promise((resolve, reject) => {
            docClient.scan(params, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    export function query<C>(Table: TableClass<C>, properties?: QueryProperties) {
        const docClient = new AWS.DynamoDB.DocumentClient(config());
        const params = {
            ...properties,
            TableName: Table.name
        };

        console.log(params);

        return new Promise((resolve, reject) => {
            docClient.query(params, (err, data) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(data);
                }
            });
        });
    }

    function config(options: any = {}) {
        if (options.convertEmptyValues === undefined) {
            options.convertEmptyValues = true;
        }

        return options;
    }

    export interface TableClass<T> {
        new(): T;
        prototype: T;
        name: string;
    }

    export interface DeleteProperties {
        Key: any;
        ConditionExpression?: string,
        ExpressionAttributeValues?: any;
        ReturnValues?: string;
    }

    export interface UpdateProperties {
        Key: any;
        ConditionExpression?: string,
        UpdateExpression?: string;
        ExpressionAttributeValues?: any;
        ReturnValues?: string;
    }

    export interface QueryProperties {
        ProjectionExpression?: string;
        KeyConditionExpression?: string;
        ExpressionAttributeNames?: any;
        ExpressionAttributeValues?: any;
    }

    export interface ScanProperties {
        ProjectionExpression?: string;
        FilterExpression?: string;
        ExpressionAttributeNames?: any;
        ExpressionAttributeValues?: any;
    }
}