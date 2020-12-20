require('dotenv').config();

import { describe, it } from 'mocha';
import { assert } from 'chai';

import { EventHubManagementClient } from '@azure/arm-eventhub';
import {
    EventHubConsumerClient,
    EventHubProducerClient,
    latestEventPosition
} from '@azure/event-hubs';

import * as REST from '@azure/ms-rest-nodeauth';


describe(`Test for https://github.com/Azure/azure-sdk-for-js/issues/12855`, function() {

    let credentials,
        mgmt_client:EventHubManagementClient,
        consumer:EventHubConsumerClient,
        producer:EventHubProducerClient;

    let subscription,
        test_id = (Math.random() * 1000000).toFixed(0),
        received_events = [],
        errors = [];

    it(`Has a valid environment`, () => {

        const env_variables = [
            'AZURE_TENANT_ID',
            'AZURE_RESOURCE_GROUP',
            'AZURE_SUBSCRIPTION_ID',
            'AZURE_CLIENT_ID',
            'AZURE_CLIENT_SECRET',
            'EVENTHUB_NAMESPACE',
            'EVENTHUB_TEST_TOPIC',
            'EVENTHUB_CONNECTION_STRING'
        ];

        env_variables.forEach( v => assert(
            typeof process.env[v] === 'string' &&
            process.env[v].length, `Missing environment variable ${v}`
        ));

    });

    it(`Logs in with service principal credentials`, async () => {

        const {
            AZURE_TENANT_ID,
            AZURE_CLIENT_ID,
            AZURE_CLIENT_SECRET
        } = process.env;

        credentials = await REST.loginWithServicePrincipalSecret(AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID);
        assert(credentials.tokenCache['_entries'].length === 1, 'failed');

    });

    describe(`Test Preparation - build out a test topic for the eventhubs namespace`, () => {

        it(`Instantiates a managment client`, async () => {

            const { AZURE_SUBSCRIPTION_ID } = process.env;
        
            mgmt_client = new EventHubManagementClient(credentials, AZURE_SUBSCRIPTION_ID);
        });

        it(`Creates the test hub for this test`, async () => {

            const {
                AZURE_RESOURCE_GROUP,
                EVENTHUB_NAMESPACE,
                EVENTHUB_TEST_TOPIC
            } = process.env;

            const result = await mgmt_client.eventHubs.createOrUpdate(
                AZURE_RESOURCE_GROUP,
                EVENTHUB_NAMESPACE,
                EVENTHUB_TEST_TOPIC,
                {}
            )

            assert(result._response.status === 200, 'failed');

        });

        it(`Instantiates a consumer`, async () => {

            const {
                EVENTHUB_TEST_TOPIC,
                EVENTHUB_CONNECTION_STRING
            } = process.env;

            consumer = await new EventHubConsumerClient(
                "$Default",
                EVENTHUB_CONNECTION_STRING,
                EVENTHUB_TEST_TOPIC
            );
        });

        it(`Instantiates a producer`, async () => {

            const {
                EVENTHUB_TEST_TOPIC,
                EVENTHUB_CONNECTION_STRING
            } = process.env;

            producer = await new EventHubProducerClient(EVENTHUB_CONNECTION_STRING, EVENTHUB_TEST_TOPIC);

        });

    });

    describe(`Test`, () => {

        it(`Instantiate the consumer subscription for latestEventPosition`, () => {

            subscription = consumer.subscribe(
                //@ts-ignore
                {
                    processEvents: ( events, context ) => {

                        if(!events.length) return
                        if(events.length > 1)
                            throw new Error(`Expected exactly 1 event, but got ${events.length}`);

                        const event = events[0]
                        context.updateCheckpoint(event);
                        received_events.push(event);
                    },
                    processError: err => {
                        errors.push(err);
                    }
                },
                { startPosition: latestEventPosition }
                //{ startPosition: { enqueuedOn: new Date() } }
            );

        });

        it(`Dispatches the message`, async () => {

            const batch = await producer.createBatch();
            const body = { test_id };
            batch.tryAdd({body});
            await producer.sendBatch(batch);
        });

        it(`Waits around for 5 seconds`, async () => {
            await new Promise(r => setTimeout(r, 5000))
        })

        it(`Did it work?`, () => {

            assert(received_events.length === 1, 'expected 1 event in the results array');
            assert(received_events[0].body.test_id === test_id, 'test ids did not match');
            assert(errors.length === 0, 'failed - got errors');
        })

    });

    describe.skip(`Test with enqueue`, () => {

        const results = [];
        const errors = [];

        it(`Instantiates the consumer subscription with enqueue`, () => {

            consumer.subscribe(
                //@ts-ignore
                {
                    processEvents: ( events, context ) => {

                        if(!events.length) return
                        if(events.length > 1)
                            throw new Error(`Expected exactly 1 event, but got ${events.length}`);

                        const event = events[0]
                        context.updateCheckpoint(event);
                        results.push(event);
                    },
                    processError: err => {
                        errors.push(err);
                    }
                },
                { startPosition: { enqueuedOn: new Date() } }
            );


        });

    })

    describe(`Cleanup`, () => {

        it(`Closes the subscription`, async () => {
            await subscription.close();
        });

        it(`Disconnects the producer, consumer and mgmt_client`, async () => {
            await producer.close();
            await consumer.close();
        })


        it(`Drops the test topic`, async () => {

            const {
                AZURE_RESOURCE_GROUP,
                EVENTHUB_NAMESPACE,
                EVENTHUB_TEST_TOPIC
            } = process.env;

            const result = await mgmt_client.eventHubs.deleteMethod(
                AZURE_RESOURCE_GROUP,
                EVENTHUB_NAMESPACE,
                EVENTHUB_TEST_TOPIC,
                {}
            )

            assert(result._response.status === 200, 'failed');
        });

    });


});