/**
 * Tests for parameterized queries with the BigQuery emulator.
 *
 * This test file specifically addresses issue #58:
 * https://github.com/Recidiviz/bigquery-emulator/issues/58
 *
 * The issue reports that numeric query parameters fail because the emulator
 * expects QueryParameterValue.Value to be a string, when BigQuery actually
 * supports numeric values directly in JSON.
 *
 * These tests verify that query parameters of various types work correctly,
 * especially numeric parameters that are sent as JSON numbers (not strings).
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import {
  BigQueryEmulatorContainer,
  BQ_EMULATOR_PROJECT_ID,
} from './utils/BigQueryEmulatorContainer.js';
import {
  createBigQueryClient,
  createTestHelper,
  BigQueryTestHelper,
} from './utils/testSetup.js';
import { BigQuery } from '@google-cloud/bigquery';

describe('Parameterized Queries (Issue #58)', () => {
  let emulator: BigQueryEmulatorContainer;
  let client: BigQuery;
  let helper: BigQueryTestHelper;

  beforeAll(async () => {
    emulator = await BigQueryEmulatorContainer.start();
    client = createBigQueryClient(emulator);
    helper = createTestHelper(client);
  });

  afterAll(async () => {
    await emulator.stop();
  });

  describe('Numeric Parameters', () => {
    beforeAll(async () => {
      // Create a test table with sample data
      await helper.createTable(
        { datasetId: 'test_dataset', tableId: 'test_table' },
        {
          fields: [
            { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'name', type: 'STRING', mode: 'REQUIRED' },
            { name: 'value', type: 'FLOAT', mode: 'NULLABLE' },
          ],
        }
      );

      // Insert test data
      await helper.insertRows(
        { datasetId: 'test_dataset', tableId: 'test_table' },
        [
          { id: 1, name: 'Alice', value: 10.5 },
          { id: 2, name: 'Bob', value: 20.7 },
          { id: 3, name: 'Charlie', value: 30.2 },
          { id: 4, name: 'David', value: 40.9 },
          { id: 5, name: 'Eve', value: 50.1 },
        ]
      );
    });

    it('should handle INT64 parameter in LIMIT clause (main issue #58 scenario)', async () => {
      // This is the exact scenario from issue #58:
      // Query with LIMIT using a numeric parameter
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        ORDER BY id ASC
        LIMIT @limit
      `;

      const [rows] = await client.query({
        query,
        params: { limit: 2 }, // Pass as number, not string
      });

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(1);
      expect(rows[0].name).toBe('Alice');
      expect(rows[1].id).toBe(2);
      expect(rows[1].name).toBe('Bob');
    });

    it('should handle INT64 parameter in WHERE clause', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE id = @id
      `;

      const [rows] = await client.query({
        query,
        params: { id: 3 }, // Numeric parameter
      });

      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe(3);
      expect(rows[0].name).toBe('Charlie');
    });

    it('should handle INT64 parameter with comparison operators', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE id >= @minId
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: { minId: 4 }, // Numeric parameter
      });

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(4);
      expect(rows[1].id).toBe(5);
    });

    it('should handle FLOAT64 parameter', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE value > @threshold
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: { threshold: 25.5 }, // Numeric (float) parameter
      });

      expect(rows).toHaveLength(3);
      expect(rows[0].name).toBe('Charlie');
      expect(rows[1].name).toBe('David');
      expect(rows[2].name).toBe('Eve');
    });

    it('should handle multiple numeric parameters', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE id >= @minId AND id <= @maxId
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: {
          minId: 2, // Numeric parameter
          maxId: 4, // Numeric parameter
        },
      });

      expect(rows).toHaveLength(3);
      expect(rows[0].id).toBe(2);
      expect(rows[1].id).toBe(3);
      expect(rows[2].id).toBe(4);
    });
  });

  describe('Mixed Parameter Types', () => {
    it('should handle mix of numeric and string parameters', async () => {
      await helper.createTable(
        { datasetId: 'test_dataset', tableId: 'mixed_table' },
        {
          fields: [
            { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'category', type: 'STRING', mode: 'REQUIRED' },
            { name: 'active', type: 'BOOLEAN', mode: 'REQUIRED' },
          ],
        }
      );

      await helper.insertRows(
        { datasetId: 'test_dataset', tableId: 'mixed_table' },
        [
          { id: 1, category: 'A', active: true },
          { id: 2, category: 'B', active: false },
          { id: 3, category: 'A', active: true },
        ]
      );

      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.mixed_table\`
        WHERE id > @minId
          AND category = @category
          AND active = @isActive
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: {
          minId: 1, // Numeric
          category: 'A', // String
          isActive: true, // Boolean
        },
      });

      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe(3);
      expect(rows[0].category).toBe('A');
    });

    it('should handle DATE parameter', async () => {
      await helper.createTable(
        { datasetId: 'test_dataset', tableId: 'date_table' },
        {
          fields: [
            { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'event_date', type: 'DATE', mode: 'REQUIRED' },
          ],
        }
      );

      await helper.insertRows(
        { datasetId: 'test_dataset', tableId: 'date_table' },
        [
          { id: 1, event_date: '2024-01-15' },
          { id: 2, event_date: '2024-02-20' },
          { id: 3, event_date: '2024-03-10' },
        ]
      );

      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.date_table\`
        WHERE event_date >= @startDate
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: {
          startDate: '2024-02-01', // Date as string
        },
      });

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(2);
      expect(rows[1].id).toBe(3);
    });
  });

  describe('Parameters in Complex Queries', () => {
    it('should handle parameters in subqueries', async () => {
      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE id IN (
          SELECT id
          FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
          WHERE value > @threshold
        )
        ORDER BY id ASC
        LIMIT @limit
      `;

      const [rows] = await client.query({
        query,
        params: {
          threshold: 30.0, // Float parameter
          limit: 2, // Int parameter
        },
      });

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(3);
      expect(rows[1].id).toBe(4);
    });

    it('should handle parameters with OFFSET', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        ORDER BY id ASC
        LIMIT @limit
        OFFSET @offset
      `;

      const [rows] = await client.query({
        query,
        params: {
          limit: 2, // Both as numbers
          offset: 2,
        },
      });

      expect(rows).toHaveLength(2);
      expect(rows[0].id).toBe(3);
      expect(rows[1].id).toBe(4);
    });
  });

  describe('Edge Cases', () => {
    it('should handle zero as a parameter value', async () => {
      const query = `
        SELECT *
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE id > @minId
        ORDER BY id ASC
      `;

      const [rows] = await client.query({
        query,
        params: { minId: 0 }, // Zero as numeric parameter
      });

      expect(rows).toHaveLength(5);
    });

    it('should handle negative numbers as parameters', async () => {
      const query = `
        SELECT @negativeValue as result
      `;

      const [rows] = await client.query({
        query,
        params: { negativeValue: -42 },
      });

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toBe(-42);
    });

    it('should handle very large integers', async () => {
      const query = `
        SELECT @largeInt as result
      `;

      const [rows] = await client.query({
        query,
        params: { largeInt: 9007199254740991 }, // Max safe integer in JS
      });

      expect(rows).toHaveLength(1);
      expect(rows[0].result).toBe(9007199254740991);
    });
  });

  describe('Issue #234: UNNEST with Array Parameters', () => {
    it('should handle UNNEST with ARRAY<STRING> parameter', async () => {
      // https://github.com/goccy/bigquery-emulator/issues/234
      // Tests that array parameters work with UNNEST
      const query = `
        SELECT *
        FROM UNNEST(@states) AS state
        ORDER BY state
      `;

      const [rows] = await client.query({
        query,
        params: {
          states: ['WA', 'WI', 'WV', 'WY'],
        },
      });

      expect(rows).toHaveLength(4);
      expect(rows[0].state).toBe('WA');
      expect(rows[1].state).toBe('WI');
      expect(rows[2].state).toBe('WV');
      expect(rows[3].state).toBe('WY');
    });

    it('should handle UNNEST with ARRAY<INT64> parameter', async () => {
      const query = `
        SELECT *
        FROM UNNEST(@numbers) AS num
        ORDER BY num
      `;

      const [rows] = await client.query({
        query,
        params: {
          numbers: [1, 2, 3, 4, 5],
        },
      });

      expect(rows).toHaveLength(5);
      expect(rows[0].num).toBe(1);
      expect(rows[4].num).toBe(5);
    });

    it('should handle UNNEST with array parameter in JOIN', async () => {
      const query = `
        SELECT t.id, t.name, state
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\` t
        CROSS JOIN UNNEST(@stateList) AS state
        WHERE t.id <= 2
        ORDER BY t.id, state
      `;

      const [rows] = await client.query({
        query,
        params: {
          stateList: ['CA', 'NY'],
        },
      });

      expect(rows).toHaveLength(4);
      expect(rows[0].id).toBe(1);
      expect(rows[0].state).toBe('CA');
      expect(rows[1].id).toBe(1);
      expect(rows[1].state).toBe('NY');
    });
  });

  describe('Issue #312: Null Parameter Handling', () => {
    it('should handle null string parameter with IS NULL check', async () => {
      // https://github.com/goccy/bigquery-emulator/issues/312
      // Tests that null parameters work correctly in IS NULL conditions
      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE @parameter IS NULL OR name = @parameter
        ORDER BY id
      `;

      const [rows] = await client.query({
        query,
        params: {
          parameter: null, // Null parameter
        },
        types: {
          parameter: 'STRING', // Must specify type for null values
        },
      });

      // Since parameter is null, the IS NULL condition is true,
      // so all rows should be returned
      expect(rows).toHaveLength(5);
      expect(rows[0].id).toBe(1);
      expect(rows[4].id).toBe(5);
    });

    it('should handle null parameter with specific value fallback', async () => {
      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE @parameter IS NULL OR name = @parameter
        ORDER BY id
      `;

      // First with null - should return all rows
      const [nullRows] = await client.query({
        query,
        params: {
          parameter: null,
        },
        types: {
          parameter: 'STRING',
        },
      });
      expect(nullRows).toHaveLength(5);

      // Then with specific value - should filter
      const [filteredRows] = await client.query({
        query,
        params: {
          parameter: 'Alice',
        },
      });
      expect(filteredRows).toHaveLength(1);
      expect(filteredRows[0].name).toBe('Alice');
    });

    it('should handle null numeric parameter', async () => {
      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE @numParam IS NULL OR id = @numParam
        ORDER BY id
      `;

      const [rows] = await client.query({
        query,
        params: {
          numParam: null,
        },
        types: {
          numParam: 'INT64',
        },
      });

      expect(rows).toHaveLength(5);
    });

    it('should handle multiple null parameters', async () => {
      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.test_table\`
        WHERE (@param1 IS NULL OR id = @param1)
          AND (@param2 IS NULL OR name = @param2)
        ORDER BY id
      `;

      const [rows] = await client.query({
        query,
        params: {
          param1: null,
          param2: null,
        },
        types: {
          param1: 'INT64',
          param2: 'STRING',
        },
      });

      // Both are null, so all rows match
      expect(rows).toHaveLength(5);
    });
  });

  describe('Issue #69: Positional Parameters', () => {
    beforeAll(async () => {
      // Create a test table for positional parameter testing
      await helper.createTable(
        { datasetId: 'test_dataset', tableId: 'positional_test' },
        {
          fields: [
            { name: 'id', type: 'INTEGER', mode: 'REQUIRED' },
            { name: 'name', type: 'STRING', mode: 'REQUIRED' },
          ],
        }
      );

      // Insert test data
      await helper.insertRows(
        { datasetId: 'test_dataset', tableId: 'positional_test' },
        [
          { id: 1, name: 'Alice' },
          { id: 2, name: 'Bob' },
          { id: 3, name: 'Charlie' },
        ]
      );
    });

    it('should handle positional parameter (?) in WHERE clause', async () => {
      // Tests resolution of https://github.com/Recidiviz/bigquery-emulator/issues/69
      // Verifies that positional query parameters work correctly and are not broken by allow_undeclared_parameters.
      // According to ZetaSQL docs: "When allow_undeclared_parameters is true, no positional parameters may be provided."

      const query = `
        SELECT id, name
        FROM \`${BQ_EMULATOR_PROJECT_ID}.test_dataset.positional_test\`
        WHERE id = ?
        ORDER BY id
      `;

      // Positional parameters are passed as an array without names
      const options = {
        query,
        params: [2], // Positional parameter value
      };

      const [rows] = await client.query(options);

      expect(rows).toHaveLength(1);
      expect(rows[0].id).toBe(2);
      expect(rows[0].name).toBe('Bob');
    });
  });
});