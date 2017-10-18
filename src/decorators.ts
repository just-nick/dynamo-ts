import { IndexOptions } from './decorators';
import { TABLE_DEFINITION, AUTO_GENERATE } from './constants';
import { DynamodbService } from "./service";
import { defaultsHelper } from "./defaults.helper";
import * as _ from 'lodash';

export function Key(options?: KeyOptions) {
    const defaultOptions: KeyOptions = {
        type: "HASH",
        autoGenerate: true
    }

    options = defaultsHelper(options, defaultOptions);

    return (target: any, propertyKey: string) => {
        const propertyType = Reflect.getMetadata('design:type', target, propertyKey);
        let tableDefinition = Reflect.getMetadata(TABLE_DEFINITION, target);

        if (!tableDefinition) {
            tableDefinition = DynamodbService.getDefaultTableDefinition();
        }

        tableDefinition.KeySchema.push({
            AttributeName: propertyKey,
            KeyType: options.type
        });

        tableDefinition.AttributeDefinitions.push({
            AttributeName: propertyKey,
            AttributeType: propertyType.name.charAt(0)
        });

        Reflect.defineMetadata(TABLE_DEFINITION, tableDefinition, target);
        if (options.autoGenerate) {
            Reflect.defineMetadata(AUTO_GENERATE, propertyKey, target);
        }
    }
}

export function Index(options?: IndexOptions) {
    return (target: any, propertyKey: string) => {
        const defaultOptions: IndexOptions = {
            name: propertyKey,
            type: "HASH"
        }

        options = defaultsHelper(options, defaultOptions);

        const propertyType = Reflect.getMetadata('design:type', target, propertyKey);
        let tableDefinition = Reflect.getMetadata(TABLE_DEFINITION, target);

        if (!tableDefinition) {
            tableDefinition = DynamodbService.getDefaultTableDefinition();
        }

        if (!tableDefinition.GlobalSecondaryIndexes) {
            tableDefinition.GlobalSecondaryIndexes = [];
        }

        let index = _.find(tableDefinition.GlobalSecondaryIndexes, (index: any) => (index.IndexName === options.name));

        if (!index) {
            index = {
                IndexName: options.name,
                KeySchema: [],
                Projection: {
                    ProjectionType: 'KEYS_ONLY'
                },
                ProvisionedThroughput: tableDefinition.ProvisionedThroughput
            }

            tableDefinition.GlobalSecondaryIndexes.push(index);
        }

        index.KeySchema.push({
            AttributeName: propertyKey,
            KeyType: options.type
        });

        tableDefinition.AttributeDefinitions.push({
            AttributeName: propertyKey,
            AttributeType: propertyType.name.charAt(0)
        });

        Reflect.defineMetadata(TABLE_DEFINITION, tableDefinition, target);
    }
}

export function Table(options?: Object) {
    return (Table: any) => {
        let tableDefinition = Reflect.getMetadata(TABLE_DEFINITION, Table.prototype);
        let autoGenerate = Reflect.getMetadata(AUTO_GENERATE, Table.prototype);

        if (!tableDefinition) {
            tableDefinition = DynamodbService.getDefaultTableDefinition();
        }

        tableDefinition.TableName = Table.name;
        Reflect.defineMetadata(AUTO_GENERATE, tableDefinition, Table.prototype);
        DynamodbService.addTable(Table, tableDefinition, autoGenerate);
    }
}

export interface KeyOptions {
    type?: "HASH" | "RANGE";
    autoGenerate?: boolean;
}

export interface IndexOptions {
    name?: string;
    type?: "HASH" | "RANGE";
}   